package com.thetradedesk.featurestore.jobs

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import com.thetradedesk.featurestore._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord, AudienceTargetingDataDataSet, AudienceTargetingDataRecord, CampaignSeedDataSet, CampaignSeedRecord}

import java.time.LocalDate
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions._

object DailySegmentDensityJob extends DensityFeatureBaseJob {

  val lalReadEnv = config.getString("lalReadEnv", readEnv)

  val lalReadOffset = config.getInt("lalReadOffset", 1)

  val lalReadWindowDays = config.getInt("lalReadWindowDays", 7)

  val writePartitions = config.getInt("writePartitions", 16)

  val outputPath = config.getString("outputPath", s"$MLPlatformS3Root/$ttdEnv/profiles/SegmentDensity/date=${getDateStr(date)}")

  def readAllLalResults(date: LocalDate): DataFrame = {
    val firstPartyLalResults = rangeReadLalResults(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/1p_all_model_unfiltered_results/v=1/idCap=100000", date)
      .withColumn("IsFirstParty", lit(true))

    val thirdPartyLalResults = rangeReadLalResults(s"s3://ttd-identity/datapipeline/$lalReadEnv/models/rsm_lal/all_model_unfiltered_results/v=1/idCap=100000", date)
      .withColumn("IsFirstParty", lit(false))

    firstPartyLalResults.union(thirdPartyLalResults)
  }

  def rangeReadLalResults(basePath: String, date: LocalDate): DataFrame = {
    val paths = (0 until lalReadWindowDays)
      .map(
        i => s"$basePath/date=${getDateStr(date.minusDays(i))}"
      )
      .filter(
        path => FSUtils.fileExists(s"$path/_SUCCESS")
      )

    spark.read.option("basePath", basePath).parquet(paths: _*)
  }

  override def runTransform(args: Array[String]): Unit = {

    val activeCampaigns = readActiveCampaigns(date)

    val audienceTargetingData = AudienceTargetingDataDataSet().readDate(date)

    val adGroups = AdGroupDataSet().readDate(date)

    val campaignSeed = CampaignSeedDataSet().readDate(date)

    // offset the lal read date due to the long delay
    val lalResults = readAllLalResults(date.minusDays(lalReadOffset))

    val segmentDensity = transform(activeCampaigns, audienceTargetingData, adGroups, campaignSeed, lalResults)

    segmentDensity
      .coalesce(writePartitions)
      .write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def transform(
                 activeCampaigns: DataFrame,
                 audienceTargetingData: Dataset[AudienceTargetingDataRecord],
                 adGroups: Dataset[AdGroupRecord],
                 campaignSeed: Dataset[CampaignSeedRecord],
                 lalResults: DataFrame // SeedId, TargetingDataId
               ): DataFrame = {

    // average lal results across days
    val smoothedLalResults = lalResults
      .groupBy("SeedId", "TargetingDataId", "IsFirstParty")
      .agg(
        avg($"RelevanceRatio").as("RelevanceRatio")
      )

    adGroups
      .join(activeCampaigns, Seq("CampaignId"), "leftsemi")
      .join(audienceTargetingData.filter($"Included" === true), Seq("AudienceId"))
      .join(campaignSeed, Seq("CampaignId"))
      .join(smoothedLalResults, Seq("SeedId", "TargetingDataId"))
      .select($"SeedId", $"TargetingDataId", $"IsFirstParty", $"RelevanceRatio".as("DensityScore"))
  }
}
