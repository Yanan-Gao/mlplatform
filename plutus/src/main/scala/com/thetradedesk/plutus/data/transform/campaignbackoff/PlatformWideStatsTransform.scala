package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.transform.SharedTransforms.AddChannel
import com.thetradedesk.plutus.data.envForWrite
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.vertica.UnifiedRtbPlatformReportDataSet
import com.thetradedesk.spark.datasets.sources.{AdFormatDataSet, AdFormatRecord, CountryDataSet, CountryRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object PlatformWideStatsTransform extends Logger {

  def getPlatformWideRegionChannelStats(platformReportData: Dataset[RtbPlatformReportCondensedData],
                                        countryData: Dataset[CountryRecord],
                                        adFormatData: Dataset[AdFormatRecord]
                                       ): Dataset[PlatformWideStatsSchema] = {

    val rtb = joinPlatformReportData(platformReportData, countryData, adFormatData)
    val rtb_agg_cp = aggregateCampaignMetrics(rtb)
    val rtb_agg = aggregatePlatWideMetrics(rtb_agg_cp)

    rtb_agg.selectAs[PlatformWideStatsSchema]
  }

  def joinPlatformReportData(platformReportData: Dataset[RtbPlatformReportCondensedData], countryData: Dataset[CountryRecord], adFormatData: Dataset[AdFormatRecord]): DataFrame = {
    val rtb = platformReportData
      .withColumn("FirstPriceAdjustment", lit(1) - (col("PredictiveClearingSavingsInUSD") / col("BidAmountInUSD")))
      .withColumn("DeviceType", col("DeviceType").cast(IntegerType))
      .withColumn("RenderingContext", col("RenderingContext").cast(IntegerType))

    // Join to country to get region
    val rtbWithRegion = rtb.alias("r")
      .join(broadcast(countryData).alias("c"), col("r.Country") === col("c.LongName"), "left")
      .select(col("r.*"), col("c.ContinentalRegionId"))

    // Get ChannelSimple
    AddChannel(rtbWithRegion, adFormatData, renderingContextCol = "RenderingContext", deviceTypeCol = "DeviceType")
  }

  def aggregateCampaignMetrics(rtb: DataFrame): DataFrame = {
    // Aggregate metrics by campaign, region & channel
    rtb.groupBy("ContinentalRegionId", "ChannelSimple", "CampaignId")
      .agg(
        sum(col("BidCount")).as("BidCount"),
        sum(col("ImpressionCount")).as("ImpressionCount"),
        sum(col("AdvertiserCostInUSD")).as("TotalSpend"),
        sum(col("PredictiveClearingSavingsInUSD")).as("TotalPredictiveClearingSavings"),
        mean(col("FirstPriceAdjustment")).as("Avg_FirstPriceAdjustment")
      )
      .withColumn("WinRate", (col("ImpressionCount") / col("BidCount")))
      .filter(col("Avg_FirstPriceAdjustment").isNotNull) // Filter out campaigns with null pc pushdowns
  }

  def aggregatePlatWideMetrics(rtb: DataFrame): DataFrame = {
    // Re-aggregate by region & channel to get platform-wide metrics
    rtb.groupBy("ContinentalRegionId", "ChannelSimple")
      .agg(
        mean(col("WinRate")).as("Avg_WinRate"),
        expr("percentile_approx(WinRate, 0.5)").alias("Med_WinRate"),
        sum(col("TotalSpend")).as("TotalSpend"),
        sum(col("TotalPredictiveClearingSavings")).as("TotalPredictiveClearingSavings"),
        mean(col("TotalPredictiveClearingSavings")).as("Avg_PredictiveClearingSavings"),
        expr("percentile_approx(TotalPredictiveClearingSavings, 0.5)").alias("Med_PredictiveClearingSavings"),
        mean(col("Avg_FirstPriceAdjustment")).as("Avg_FirstPriceAdjustment"),
        expr("percentile_approx(Avg_FirstPriceAdjustment, 0.5)").alias("Med_FirstPriceAdjustment")
      )
  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    val platformReportData = UnifiedRtbPlatformReportDataSet.readRange(date, date.plusDays(1))
      .selectAs[RtbPlatformReportCondensedData]

    val countryData = CountryDataSet().readLatestPartitionUpTo(date)
    val adFormatData = AdFormatDataSet().readLatestPartitionUpTo(date)

    val platformwide_agg = getPlatformWideRegionChannelStats(platformReportData, countryData, adFormatData)

    val outputPath = PlatformWideStatsDataset.S3_PATH_DATE(date, envForWrite)
    platformwide_agg.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
