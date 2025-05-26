package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.jobs.SeedPriority
import com.thetradedesk.featurestore.rsm.CommonEnums.{CrossDeviceVendor, DataSource}
import com.thetradedesk.featurestore.transform._
import com.thetradedesk.featurestore.utils.SIBSampler
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{CampaignSeedDataSet, SeedDetailDataSet}
import com.thetradedesk.spark.datasets.sources.provisioning.CampaignFlightDataSet
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object DailyTDIDDensityScoreSplitJobParameterized extends DensityFeatureBaseJob {

  override val jobName: String = "DailyTDIDDensityScoreSplitJob"

  val repartitionNum = 65520

  def readTDIDFeaturePairMappingRDD(date: LocalDate, featurePair: String, splitIndex: Int, numPartitions: Int) = {
    val featurePairToTDIDDF = spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=DailyTDIDFeaturePairMapping/config=${featurePair}/v=1/date=${getDateStr(date)}")
      .select(s"${featurePair}Hashed", "TDID")
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
      .select(s"${featurePair}Hashed", "TDID")
  }

  def readSyntheticIdDensityCategories(date: LocalDate) = {
    spark.read.parquet(s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=FeatureKeyValue/job=DailyDensityScoreReIndexingJob/config=SyntheticIdDensityScoreCategorized/date=${getDateStr(date)}")
      .repartition(repartitionNum, 'FeatureValueHashed)
      .cache()
  }

  override def runTransform(args: Array[String]) = {
    val dateStr = getDateStr(date)
    val numPartitions = 10

    val mergeDensityLevels = udaf(MergeDensityLevelAgg)
    val syntheticIdDensityCategories = readSyntheticIdDensityCategories(date)

    // Get active seed ids
    val campaignStartDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(campaignFlightStartingBufferInDays))
    val campaignEndDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)
    val newSeedStartDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.minusDays(newSeedBufferInDays))
    val activeCampaign = CampaignFlightDataSet()
      .readLatestPartition()
      .where('IsDeleted === false
        && 'StartDateInclusiveUTC.leq(campaignStartDateTimeStr)
        && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(campaignEndDateTimeStr)))
      .select('CampaignId)
      .distinct()

    val newSeed = SeedDetailDataSet()
      .readLatestPartition()
      .where(to_timestamp('CreatedAt, "yyyy-MM-dd HH:mm:ss").gt(newSeedStartDateTimeStr))
      .select('SeedId)
      .withColumn("SeedPriority", lit(SeedPriority.NewSeed.id))

    val campaignSeed = CampaignSeedDataSet()
      .readLatestPartition()

    // active campaign seed >> new seed >> inactive campaign seed >> other
    val seedPriority = campaignSeed
      .join(activeCampaign.withColumn("SeedPriority", lit(SeedPriority.ActiveCampaignSeed.id)), Seq("CampaignId"), "left")
      .withColumn("SeedPriority", coalesce('SeedPriority, lit(SeedPriority.CampaignSeed.id)))
      .groupBy('SeedId)
      .agg(
        max('SeedPriority).as("SeedPriority")
      )
      .unionByName(newSeed)
      .groupBy('SeedId)
      .agg(
        max('SeedPriority).as("SeedPriority")
      )


    val syntheticIdToMappingId =
      spark.sparkContext.broadcast(readPolicyTable(date.minusDays(1), DataSource.Seed.id)
        .join(seedPriority, Seq("SeedId"), "left")
        .select('SyntheticId, 'MappingId, coalesce('SeedPriority, lit(SeedPriority.Other.id)).as("SeedPriority"))
        .as[(Int, Int, Int)]
        .map(e => (e._1, (e._2, e._3)))
        .collect()
        .toMap)

    val filterBySourceSyntheticUdf = udf(
      (pairs: Seq[Int], keepSeed: Boolean) => {
        pairs.filter(e => syntheticIdToMappingId.value.contains(e) == keepSeed)
      }
    )

    val sourceTypeToKeepSeed = mutable.HashMap[String, Boolean](
      DataSource.Seed.toString -> true,
      DataSource.TTDOwnData.toString -> false
    )

    val syntheticIdToMappingIdUdf = udf(
      (pairs: Seq[Int], maxLength: Int) =>
      {
        val priorityCount = Array.fill(SeedPriority.values.size)(0)
        val result = pairs.flatMap(e => {
          val v = syntheticIdToMappingId.value.get(e)
          // count how many mapping ids in different priorities
          if (v.nonEmpty) priorityCount.update(v.get._2, priorityCount(v.get._2) + 1)
          v
        })
        if (pairs.length <= maxLength) {
          result.map(_._1)
        }
        else {
          var resultCount = 0
          var currentIndex = 0
          breakable {
            for (i <- priorityCount.indices) {
              resultCount = resultCount + priorityCount(i)
              if (resultCount >= maxLength) {
                currentIndex = i
                break
              }
            }
          }

          if (resultCount == maxLength) {
            result.filter(e => e._2 <= currentIndex).map(_._1)
          } else {
            val lowerPriorityResult = result.filter(e => e._2 < currentIndex).map(_._1)
            val currentPriorityResult = Random.shuffle(result.filter(e => e._2 == currentIndex).map(_._1)).take(maxLength - resultCount + priorityCount(currentIndex))
            lowerPriorityResult ++ currentPriorityResult
          }
        }
      }
    )

    val maxNumMappingIdsInAerospike = config.getInt("maxNumMappingIdsInAerospike", default = 1500)

    def processSplit(splitIndex: Int): Unit = {
      val tdidFeaturePairMappings = featurePairStrings.map(featurePair => (featurePair, readTDIDFeaturePairMappingRDD(date, featurePair, splitIndex, numPartitions)))

      val writePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=$jobName/v=1/date=$dateStr/split=$splitIndex"
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
          .select('TDID, 'SyntheticIdLevel1, 'SyntheticIdLevel2)
          .groupBy('TDID)
          .agg(mergeDensityLevels('SyntheticIdLevel1, 'SyntheticIdLevel2).as("x"))
          .cache()

        cachedDF = cachedDF :+ tempDF

        sourceTypeToKeepSeed.foreach(
          e => {
            val subWritePath = s"$MLPlatformS3Root/$ttdEnv/profiles/source=bidsimpression/index=TDID/job=${jobName}Sub/Source=${e._1}/FeatureKey=$featurePair/v=1/date=$dateStr/split=$splitIndex"

            tempDF
              .select('TDID,
                filterBySourceSyntheticUdf(col("x._1"), lit(e._2)).as("SyntheticId_Level1"),
                filterBySourceSyntheticUdf(col("x._2"), lit(e._2)).as("SyntheticId_Level2")
              )
              .write.mode(SaveMode.Overwrite).parquet(subWritePath)
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

      val tdidDensityScore = tdidFeaturePairDensityScore
        .reduce(_ union _)
        .groupBy('TDID)
        .agg(
          flatten(collect_list(col("SyntheticId_Level1"))).alias("SyntheticId_Level1"),
          flatten(collect_list(col("SyntheticId_Level2"))).alias("SyntheticId_Level2"),
        )
        .withColumn("MappingIdLevel2", syntheticIdToMappingIdUdf('SyntheticId_Level2, lit(maxNumMappingIdsInAerospike)))
        .withColumn("MappingIdLevel1", syntheticIdToMappingIdUdf('SyntheticId_Level1, lit(maxNumMappingIdsInAerospike) - size('MappingIdLevel2)))
        .withColumn("MappingIdLevel2", MappingIdSplitUDF(0)('MappingIdLevel2))
        .withColumn("MappingIdLevel2S1", MappingIdSplitUDF(1)('MappingIdLevel2))
        .withColumn("MappingIdLevel1", MappingIdSplitUDF(0)('MappingIdLevel1))
        .withColumn("MappingIdLevel1S1", MappingIdSplitUDF(1)('MappingIdLevel1))

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