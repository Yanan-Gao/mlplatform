package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.AdGroupDataSet
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

import java.time.LocalDate

object AudienceCalculateDistributionParameters {
  val prometheus = new PrometheusClient("AudienceModelJob", "AudienceCalculateDistributionParametersJob")

  object Config {
    val sourceDataFolder = config.getStringRequired("sourceDataFolder")
    val targetDataFolder = config.getStringRequired("targetDataFolder")
    val IALFolder = config.getStringRequired("IALFolder")
    val campaignSeedMappingFolder = config.getStringRequired("campaignSeedMappingFolder")
    val policyFolder = config.getStringRequired("policyFolder")
    val writeMode = config.getString("writeMode", "error")
    val date = config.getDate("date", LocalDate.now())
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {

    val sourceDF = spark.read.parquet(Config.sourceDataFolder)
    // campaign seed mapping : s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignseed/v=1/date=/
    val campaignSeedDF = spark.read.parquet(Config.campaignSeedMappingFolder)
    // policy folder: s3://thetradedesk-mlplatform-us-east-1/configdata/prod/audience/policyTable/RSM/v=1/20241101000000
    val policyDF = spark.read.parquet(Config.policyFolder)
    val ialDF = spark.read.parquet(Config.IALFolder)
    val adgroupDF = AdGroupDataSet().readLatestPartitionUpTo(Config.date, true)

    val minMaxDF = ialDF.select(explode($"AdGroupCandidates").alias("AdGroupCandidate"))
      .withColumn("AdGroupId", $"AdGroupCandidate.AdGroupId")
      .join(adgroupDF
        .select($"AdGroupId", $"CampaignId"), Seq("AdgroupId"), "inner")
      .join(campaignSeedDF, Seq("CampaignId"), "inner")
      .join(policyDF.filter($"CrossDeviceVendorId"===0), $"SeedId"===$"SourceId", "inner")
      .select($"AdGroupCandidate.RelevanceScore".as("rawRelevanceScore"), $"SyntheticId")
      .groupBy("SyntheticId")
      .agg(
        min($"rawRelevanceScore").cast(FloatType).as("minScore")
        , max("rawRelevanceScore").cast(FloatType).as("maxScore")
      )
      .withColumn("minMaxDiff", $"maxScore"-$"minScore")
      .filter($"minMaxDiff">0)
      .select($"SyntheticId", array(List(col("maxScore"), col("minMaxDiff")): _*).alias("Thresholds"))


    sourceDF.join(
        minMaxDF, Seq("SyntheticId"), "left"
      )
      .withColumn("Threshold", lit(0.5).cast(FloatType))
      .write.mode(Config.writeMode).parquet(Config.targetDataFolder)
  }
}
