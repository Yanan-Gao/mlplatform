package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.{AdGroupDataSet, AudienceModelPolicyReadableDataset, AudienceModelThresholdRecord, AudienceModelThresholdWritableDataset, CampaignSeedDataset, Model}
import com.thetradedesk.audience.{date, dateTime}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object AudienceCalculateThresholdsJob {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceCalculateThresholdsJob")

  object Config {
    val model = Model.withName(config.getString("modelName", default = "RSM"))
    val IALFolder = config.getStringRequired("IALFolder")
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {

    val campaignSeedDF = CampaignSeedDataset().readLatestPartition()
    // policy folder: s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/20241101000000
    val policyDF = AudienceModelPolicyReadableDataset(Model.RSM).readSinglePartition(dateTime)(spark)
    val ialDF = spark.read.parquet(Config.IALFolder)
    val adgroupDF = AdGroupDataSet().readLatestPartitionUpTo(date, true)

    val minMaxDF = ialDF.select(explode($"AdGroupCandidates").alias("AdGroupCandidate"))
      .withColumn("AdGroupId", $"AdGroupCandidate.AdGroupId")
      .filter(($"AdGroupCandidate.RelevanceResultSource" === lit(1)) && ($"AdGroupCandidate.RelevanceModel" === lit(1)))
      .join(adgroupDF
        .select($"AdGroupId", $"CampaignId"), Seq("AdgroupId"), "inner")
      .join(campaignSeedDF, Seq("CampaignId"), "inner")
      .join(policyDF.filter($"CrossDeviceVendorId" === 0), $"SeedId" === $"SourceId", "inner")
      .select($"AdGroupCandidate.RelevanceScore".as("rawRelevanceScore"), $"SyntheticId")
      .groupBy("SyntheticId")
      .agg(
        min($"rawRelevanceScore").cast(FloatType).as("minScore")
        , max("rawRelevanceScore").cast(FloatType).as("maxScore")
      )
      .withColumn("minMaxDiff", $"maxScore" - $"minScore")
      .filter($"minMaxDiff" > 0)
      .select($"SyntheticId", array(List(col("maxScore"), col("minMaxDiff")): _*).alias("Thresholds"))
      .withColumn("Threshold", lit(0.5).cast(FloatType))
      .as[AudienceModelThresholdRecord]

    AudienceModelThresholdWritableDataset(Config.model)
      .writePartition(minMaxDF, dateTime,
        saveMode = SaveMode.Overwrite)
  }
}
