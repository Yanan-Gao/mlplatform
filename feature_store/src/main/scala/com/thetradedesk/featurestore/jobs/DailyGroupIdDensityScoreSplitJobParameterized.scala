package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform.MergeDensityLevelAgg
import com.thetradedesk.featurestore.utils.CrossDeviceGraphUtil
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DailyGroupIdDensityScoreSplitJobParameterized extends DensityFeatureBaseJob {
  override val jobName: String = "DailyGroupIDDensityScoreSplitJob"
  val repartitionNum = 65520

  override def runTransform(args: Array[String]): Unit = {
    val densityFeatureLevel2Threshold = config.getDouble("densityFeatureLevel2Threshold", default = 0.05).floatValue()
    val mergeDensityLevels = udaf(MergeDensityLevelAgg(densityFeatureLevel2Threshold))
    val availableSyntheticIds = spark.sparkContext.broadcast(
      readPolicyTable(date, DataSource.Seed.id)
        .select('SyntheticId)
        .as[Int]
        .collect()
        .toSet
    )

    val filterSeedSyntheticIdsUdf = udf(
      (pairs: Seq[Int]) => {
        pairs.filter(e => availableSyntheticIds.value.contains(e))
      }
    )

    val tdidSubDensityFeature = readTDIDDensityFeature(date)
      .select('TDID
        , filterSeedSyntheticIdsUdf('SyntheticId_Level1).as("SyntheticId_Level1")
        , filterSeedSyntheticIdsUdf('SyntheticId_Level2).as("SyntheticId_Level2"))
      .repartition(repartitionNum, 'TDID)

    val xDevice = Seq(CrossDeviceVendor.IAV2Person)
      .map(e => (e, CrossDeviceGraphUtil
        .readGraphData(date, e)
        .withColumn("GroupCount", count("*").over(Window.partitionBy("GroupId")))))

    xDevice.foreach {
      case (crossDeviceVendor: CrossDeviceVendor, graphDF: DataFrame) =>
        val writePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=GroupId/job=DailyGroupIDDensityScoreSplitJob/XDV=${crossDeviceVendor.toString}/v=1/date=${getDateStr(date)}"
        val successFile = s"$writePath/_SUCCESS"

        // skip processing this split if data from a previous run already exists
        if (overrideOutput || !FSUtils.fileExists(successFile)(spark)) {
          tdidSubDensityFeature
            .join(graphDF
              .repartition(repartitionNum, 'TDID), Seq("TDID"))
            .groupBy("GroupId")
            .agg(mergeDensityLevels('SyntheticId_Level1, 'SyntheticId_Level2, lit(1), 'GroupCount).as("x"))
            .select('GroupId, col("x._1").as("SyntheticId_Level1"), col("x._2").as("SyntheticId_Level2"))
            .write.mode(SaveMode.Overwrite)
            .parquet(writePath)
        } else {
          println(s"crossDeviceVendor ${crossDeviceVendor.toString} data is existing")
        }

        val exportTDIDLevelGraphDensityFeature = config.getBoolean("exportTDIDLevelGraphDensityFeature", default = false)

        if (exportTDIDLevelGraphDensityFeature) {
          val groupDF = spark.read.parquet(writePath)
            .repartition(repartitionNum, 'GroupId).cache()

          (0 until numSplits).foreach(
            e => {
              val subWritePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=TDID/job=DailyGroupIDDensityScoreSplitJob/XDV=${crossDeviceVendor.toString}/v=1/date=${getDateStr(date)}/split=$e"
              val subSuccessFile = s"$subWritePath/_SUCCESS"

              // skip processing this split if data from a previous run already exists
              if (overrideOutput || !FSUtils.fileExists(subSuccessFile)(spark)) {
                groupDF
                  .join(graphDF
                    .where(abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(numSplits) === lit(e))
                    .repartition(repartitionNum, 'GroupId)
                    , Seq("GroupId"))
                  .write.mode(SaveMode.Overwrite)
                  .parquet(subWritePath)
              } else {
                println(s"crossDeviceVendor ${crossDeviceVendor.toString} split $e breakdown data is existing")
              }
            }
          )
        }
    }
  }
}
