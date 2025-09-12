package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.rsm.CommonEnums.DataSource
import com.thetradedesk.featurestore.transform._
import com.thetradedesk.featurestore.utils.SIBSampler
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import scala.collection.mutable

object DailyTDIDDensityScoreSplitJobParameterized extends DensityFeatureBaseJob {

  override val jobName: String = "DailyTDIDDensityScoreSplitJob"

  // configuration overrides
  val tdidFeaturePairEnv: String = config.getString("tdidFeaturePairEnv", ttdEnv)
  val syntheticIdDensityEnv: String = config.getString("syntheticIdDensityEnv", ttdEnv)

  val repartitionNum = 65520

  def readTDIDFeaturePairMappingRDD(date: LocalDate, featurePair: String, splitIndex: Int, numPartitions: Int) = {
    val featurePairToTDIDDF = spark.read.parquet(s"$MLPlatformS3Root/$tdidFeaturePairEnv/profiles/source=bidsimpression/index=TDID/job=DailyTDIDFeaturePairMapping/config=${featurePair}/v=1/date=${getDateStr(date)}")
      .select(s"${featurePair}Hashed", "TDID", "FeatureFrequency", "UserFrequency")
      .cache()

    val condition =
      if (splitIndex < 0) {
        SIBSampler.isDeviceIdSampled1Percent(col("TDID"))
      } else {
        col("TDID").substr(9, 1) === lit("-") &&
          (abs(xxhash64(concat(col("TDID"), lit(salt)))) % lit(numPartitions) === lit(splitIndex))
      }

    featurePairToTDIDDF
      .filter(condition)
  }

  def readSyntheticIdDensityCategories(date: LocalDate) = {
    spark.read.parquet(s"$MLPlatformS3Root/$syntheticIdDensityEnv/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized/date=${getDateStr(date)}")
      .repartition(repartitionNum, 'FeatureValueHashed)
      .cache()
  }

  override def runTransform(args: Array[String]) = {
    val dateStr = getDateStr(date)
    val numPartitions = 10

    val densityFeatureLevel2Threshold = config.getDouble("densityFeatureLevel2Threshold", default = 0.05).floatValue()
    val mergeDensityLevels = udaf(MergeDensityLevelAgg(densityFeatureLevel2Threshold))
    val syntheticIdDensityCategories = readSyntheticIdDensityCategories(date)
    val policyData = readPolicyTable(date, DataSource.Seed.id)

    val seedPriority = getSeedPriority

    val syntheticIdToMappingId = getSyntheticIdToMappingId(policyData, seedPriority)

    val filterBySourceSyntheticUdf = udf(
      (pairs: Seq[Int], keepSeed: Boolean) => {
        pairs.filter(e => syntheticIdToMappingId.value.contains(e) == keepSeed)
      }
    )

    val sourceTypeToKeepSeed = mutable.HashMap[String, Boolean](
      DataSource.Seed.toString -> true,
      DataSource.TTDOwnData.toString -> false
    )

    val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

    def processSplit(splitIndex: Int): Unit = {
      val tdidFeaturePairMappings = featurePairStrings.map(featurePair => (featurePair, readTDIDFeaturePairMappingRDD(date, featurePair, splitIndex, numPartitions)))

      val writePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=TDID/job=$jobName/v=1/date=$dateStr/split=$splitIndex"
      val successFile = s"$writePath/_SUCCESS"

      // skip processing this split if data from a previous run already exists
      if (!overrideOutput && FSUtils.fileExists(successFile)(spark)) {
        println(s"split ${splitIndex} data is existing")
        return
      }

      var cachedDF: Seq[DataFrame] = Seq()

      val tdidFeaturePairDensityScore = tdidFeaturePairMappings.map { case (featurePair, tdidMapping) =>
        val tempDF = tdidMapping.withColumnRenamed(s"${featurePair}Hashed", "FeatureValueHashed")
          .repartition(repartitionNum, 'FeatureValueHashed)
          .join(syntheticIdDensityCategories.filter(col("FeatureKey") === lit(featurePair)), Seq("FeatureValueHashed"))
          .groupBy('TDID)
          .agg(mergeDensityLevels('SyntheticIdLevel1, 'SyntheticIdLevel2, 'FeatureFrequency, 'UserFrequency).as("x"))
          .cache()

        cachedDF = cachedDF :+ tempDF

        sourceTypeToKeepSeed.foreach(
          e => {
            val subWritePath = s"$MLPlatformS3Root/$writeEnv/profiles/source=bidsimpression/index=TDID/job=${jobName}Sub/Source=${e._1}/FeatureKey=$featurePair/v=1/date=$dateStr/split=$splitIndex"

            if (e._2) {
              tempDF
                .select('TDID,
                  filterBySourceSyntheticUdf(col("x._1"), lit(e._2)).as("SyntheticId_Level1"),
                  filterBySourceSyntheticUdf(col("x._2"), lit(e._2)).as("SyntheticId_Level2")
                )
                .write.mode(SaveMode.Overwrite).parquet(subWritePath)
            } else {
              tempDF
                .select('TDID,
                  filterBySourceSyntheticUdf(col("x._1"), lit(e._2)).as("SyntheticId_Level1"),
                  filterBySourceSyntheticUdf(col("x._2"), lit(e._2)).as("SyntheticId_Level2")
                )
                .coalesce(1000)
                .write.mode(SaveMode.Overwrite).parquet(subWritePath)
            }
          }
        )

        // keep the logic the same as the existing one
        if (featurePair == nonSensitiveFeaturePair) {
          tempDF.select('TDID,
            col("x._1").as("SyntheticId_Level1"),
            col("x._2").as("SyntheticId_Level2"))
        } else {
          tempDF.select('TDID,
            filterBySourceSyntheticUdf(col("x._1"), lit(true)).as("SyntheticId_Level1"),
            filterBySourceSyntheticUdf(col("x._2"), lit(true)).as("SyntheticId_Level2"))
        }
      }

      val syntheticIdToMappingIdUdf = getSyntheticIdToMappingIdUdf(syntheticIdToMappingId.value)

      val tdidDensityScore = splitDensityScoreSlots(
        tdidFeaturePairDensityScore
          .reduce(_ union _)
          .groupBy('TDID)
          .agg(
            flatten(collect_list(col("SyntheticId_Level1"))).alias("SyntheticId_Level1"),
            flatten(collect_list(col("SyntheticId_Level2"))).alias("SyntheticId_Level2"),
          )
          .withColumn("MappingIdLevel2", syntheticIdToMappingIdUdf('SyntheticId_Level2, lit(maxNumMappingIdsInAerospike)))
          .withColumn("MappingIdLevel1", syntheticIdToMappingIdUdf('SyntheticId_Level1, lit(maxNumMappingIdsInAerospike) - size('MappingIdLevel2)))
      )

      tdidDensityScore.write.mode(SaveMode.Overwrite).parquet(writePath)
      cachedDF.foreach(_.unpersist())
    }

    splitIndex.foreach(
      processSplit
    )

    syntheticIdDensityCategories.unpersist()
  }
}

object SeedPriority extends Enumeration {
  type SeedPriority = Value
  val ActiveCampaignSeed, NewSeed, CampaignSeed, Other = Value
}