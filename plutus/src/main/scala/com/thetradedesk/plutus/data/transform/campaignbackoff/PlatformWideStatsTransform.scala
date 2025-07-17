package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.transform.SharedTransforms.{AddChannel, AddDeviceTypeIdAndRenderingContextId}
import com.thetradedesk.plutus.data.envForWrite
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.vertica.UnifiedRtbPlatformReportDataSet
import com.thetradedesk.spark.datasets.sources.{CountryDataSet, CountryRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object PlatformWideStatsTransform extends Logger {

  def getPlatformWideRegionChannelStats(platformReportData: Dataset[RtbPlatformReportCondensedData],
                                        countryData: Dataset[CountryRecord]
                                       ): Dataset[PlatformWideStatsSchema] = {

    val rtb = joinPlatformReportData(platformReportData, countryData)
    val rtb_agg_cp = aggregateCampaignMetrics(rtb)
    val rtb_agg = aggregatePlatWideMetrics(rtb_agg_cp)

    rtb_agg.selectAs[PlatformWideStatsSchema]
  }

  def joinPlatformReportData(platformReportData: Dataset[RtbPlatformReportCondensedData], countryData: Dataset[CountryRecord]): DataFrame = {
    val rtbWithRegion = platformReportData
      .withColumn("FirstPriceAdjustment", lit(1) - (col("PredictiveClearingSavingsInUSD") / col("BidAmountInUSD")))
      // Join to Country to get Region
      .join(broadcast(countryData.select(col("LongName").as("Country"), col("ContinentalRegionId"))), Seq("Country"), "left")

    // Get ChannelSimple - requires translating the strings to their numeric ids
    val addRenderingContextAndDeviceTypeId = AddDeviceTypeIdAndRenderingContextId(rtbWithRegion, renderingContextCol = "RenderingContext", deviceTypeCol = "DeviceType")
    AddChannel(addRenderingContextAndDeviceTypeId, renderingContextCol = "RenderingContextId", deviceTypeCol = "DeviceTypeId")
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
      .withColumn("WinRate", col("ImpressionCount") / col("BidCount"))
      // Filter out campaigns with null pc pushdowns
      .filter(col("Avg_FirstPriceAdjustment").isNotNull)
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

    val platformwide_agg = getPlatformWideRegionChannelStats(platformReportData, countryData)

    PlatformWideStatsDataset.writeData(date, platformwide_agg, fileCount)
  }
}
