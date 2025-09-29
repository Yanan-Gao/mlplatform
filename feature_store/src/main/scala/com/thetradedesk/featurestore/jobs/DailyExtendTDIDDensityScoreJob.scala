package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform.MappingIdSplitUDF
import com.thetradedesk.featurestore.utils.CrossDeviceGraphUtil
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object DailyExtendTDIDDensityScoreJob extends DensityFeatureBaseJob {


  // configuration overrides
  val groupIDDensityScoreEnv: String = config.getString("groupIDDensityScoreEnv", ttdEnv)

  val repartitionNum = 65520
  val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

  def readGroupIdDensityFeatures(crossDeviceVendor: CrossDeviceVendor, policyData: DataFrame): DataFrame = {
    val availableSyntheticIds = spark.sparkContext.broadcast(
      policyData
        .filter('NeedGraphExtension)
        .select('SyntheticId)
        .as[Int]
        .collect()
        .toSet
    )

    val filterSeedSyntheticIdsUdf = udf(
      (syntheticIds: Seq[Int]) => {
        syntheticIds.filter(e => availableSyntheticIds.value.contains(e))
      }
    )
    spark.read.parquet(s"$MLPlatformS3Root/$groupIDDensityScoreEnv/profiles/source=bidsimpression/index=GroupId/job=DailyGroupIDDensityScoreSplitJob/XDV=${crossDeviceVendor.toString}/v=1/date=${getDateStr(date)}")
      .select('GroupId
        , filterSeedSyntheticIdsUdf('SyntheticId_Level1).as("Graph_SyntheticId_Level1")
        , filterSeedSyntheticIdsUdf('SyntheticId_Level2).as("Graph_SyntheticId_Level2"))
      .filter(
        size(coalesce('Graph_SyntheticId_Level1, array())) > 0 ||
          size(coalesce('Graph_SyntheticId_Level2, array())) > 0
      )
    //      .repartition(repartitionNum, 'GroupId)
  }

  override def runTransform(args: Array[String]): Unit = {
    val policyData = readPolicyTable(date, DataSource.Seed.id)

    val xDevice = Seq(CrossDeviceVendor.IAV2Person)
      .map(e => (e, CrossDeviceGraphUtil
        .readGraphData(date, e)
      ))

    val seedPriority = getSeedPriority
    val syntheticIdToMappingId = getSyntheticIdToMappingId(policyData, seedPriority)

    xDevice.foreach {
      case (crossDeviceVendor: CrossDeviceVendor, graphDF: DataFrame) =>

        val groupIdDensityFeature = readGroupIdDensityFeatures(crossDeviceVendor, policyData)

        // aggregate group features by tdid
        val aggregatedGroupFeaturesByTdid = graphDF
          .join(groupIdDensityFeature, "GroupId")
          .cache()


        splitIndex.foreach(x => {

          val writePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=TDID/job=DailyExtendTDIDDensityScoreJob/XDV=${crossDeviceVendor.toString}/v=1/date=${
            getDateStr(date)
          }/split=${x}"

          println(s"Writing to $writePath")
          val successFile = s"$writePath/_SUCCESS"

          // skip processing this split if data from a previous run already exists
          if (overrideOutput || !FSUtils.fileExists(successFile)(spark)) {

            // read TDID density feature
            val tdidDensityFeature = readTDIDDensityFeature(date, x)
            //              .repartition(repartitionNum, 'TDID)

            val result = extendDensityFeature(tdidDensityFeature, aggregatedGroupFeaturesByTdid, syntheticIdToMappingId.value)

            result.write.mode(SaveMode.Overwrite).parquet(writePath)
          } else {
            println(s"Data is existing at ${writePath}")
          }
        })

        aggregatedGroupFeaturesByTdid.unpersist()
    }
  }

  val remainingQuotaCol = "remainingQuotaCol"

  def extendDensityFeature(tdidDensityFeature: DataFrame, aggregatedGroupFeaturesByTdid: DataFrame, syntheticIdToMappingId: Map[Int, (Int, Int)]): DataFrame = {
    tdidDensityFeature
      .join(aggregatedGroupFeaturesByTdid, Seq("TDID"), "left")
      .withColumn(remainingQuotaCol, lit(maxNumMappingIdsInAerospike))
      .transform(df => buildSyntheticAndMapping(df, 1, "Graph_SyntheticId", "SyntheticId_Level1", syntheticIdToMappingId))
      .transform(df => buildSyntheticAndMapping(df, 2, "Graph_SyntheticId", "SyntheticId_Level2", syntheticIdToMappingId))
      .drop(remainingQuotaCol)
  }

  def buildSyntheticAndMapping(
                                df: DataFrame,
                                level: Int,
                                groupSyntheticCol: String,
                                tdidSyntheticCol: String,
                                syntheticIdToMappingId: Map[Int, (Int, Int)]
                              ): DataFrame = {


    val syntheticIdToMappingIdUdf = getSyntheticIdToMappingIdUdf(syntheticIdToMappingId)

    val synCol = s"${groupSyntheticCol}_Level$level"
    val groupMapCol = s"GraphMappingIdLevel$level"
    val groupMapColS1 = s"${groupMapCol}S1"

    df
      // group synthetic column, except TDID IDs
      .withColumn(synCol,
        when(col(synCol).isNotNull,
          array_distinct(array_except(col(synCol), col(tdidSyntheticCol)))
        ).otherwise(lit(null))
      )
      // mapping col
      .withColumn(groupMapCol,
        when(col(synCol).isNotNull,
          syntheticIdToMappingIdUdf(col(synCol), col(remainingQuotaCol))
        ).otherwise(lit(null))
      )
      // mapping split
      .withColumn(groupMapColS1,
        when(col(groupMapCol).isNotNull, MappingIdSplitUDF(1)(col(groupMapCol))).otherwise(lit(null))
      )
      .withColumn(groupMapCol,
        when(col(groupMapCol).isNotNull, MappingIdSplitUDF(0)(col(groupMapCol))).otherwise(lit(null))
      )
      .withColumn(remainingQuotaCol, col(remainingQuotaCol) - size(col(groupMapColS1)) - size(col(groupMapCol)))
  }
}
