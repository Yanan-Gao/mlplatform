package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.constants.FeatureConstants._
import com.thetradedesk.featurestore.transform.Loader._
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataSetExtensions}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import org.apache.spark.sql.functions._


/** Join daily attribution back to BidsImpression data for CPA and ROAS
 *
 *  Lookback window is defined by `convLookback`. [T-convLookback, T] BidsImpression is used for attribution
 */
object GenDailyConvertedImpressions extends FeatureStoreBaseJob {

  override def jobName: String = "genDailyConvertedImpression"

  val convLookback = config.getInt("convLookback", 15)

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // load CCRC table to filter down to attribution TrackingTags
    val ccrcProcessed = loadValidTrackingTag(date)
    // filter down to CPA & Roas campaigns
    val campaignROIGoal = CampaignROIGoalDataSet().readLatestPartitionUpTo(date, true)
      .filter(($"Priority" === 1) && ($"ROIGoalTypeId".isin(List(ROIGoalTypeId_CPA, ROIGoalTypeId_ROAS): _*))).selectAs[CampaignROIGoalRecord]

    // load all trackingtag data for current day
    val dailyAttribution = DailyAttributionDataset().readDate(date)

    // load bidfeedback & click
    val dailyClickBF = DailyClickBidFeedbackDataset().readRange(date.minusDays(convLookback - 1).atStartOfDay(),
      date.atStartOfDay(), isInclusive = true
    )

    val validAttribution = dailyAttribution.join(
      broadcast(ccrcProcessed.withColumnRenamed("ReportingColumnId", "CampaignReportingColumnId")),
      Seq("CampaignId", "TrackingTagId", "CampaignReportingColumnId"),
      "left_semi"
    ).filter(
      $"AttributedEventLogEntryTime".isNotNull && (
        (unix_timestamp($"ConversionTrackerLogEntryTime") - unix_timestamp($"AttributedEventLogEntryTime")) <= convLookback * 86400
      )
    )

    val attributedEvents = validAttribution.filter($"AttributedEventTypeId" === lit(AttributedEventTypeId_View))
      .join(
        dailyClickBF.withColumnRenamed("BidFeedbackId", "AttributedEventId"),
        Seq("AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId"),
        "inner"
      ).select("AdGroupId", "CampaignId", "AdvertiserId", "BidRequestId", "TrackingTagId")
      .union(
        validAttribution.filter($"AttributedEventTypeId" === lit(AttributedEventTypeId_Click))
          .join(
            dailyClickBF.withColumnRenamed("ClickRedirectId", "AttributedEventId").filter($"AttributedEventId".isNotNull),
            Seq("AttributedEventId", "AdGroupId", "CampaignId", "AdvertiserId"),
            "inner"
          ).select("AdGroupId", "CampaignId", "AdvertiserId", "BidRequestId", "TrackingTagId")
      ).distinct().cache()

    validAttribution.unpersist()

    // load bidsimpression
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](
      bidImpressionsS3Path,
      date,
      lookBack = Some(convLookback - 1),
      source = Some(GERONIMO_DATA_SOURCE)
    )
    val parsedBidsImp = bidsImpressions.join(
      broadcast(campaignROIGoal), Seq("CampaignId"), "left_semi"
    ).withColumn("AdFormat", concat(col("AdWidthInPixels"), lit('x'), col("AdHeightInPixels")))
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamily.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .filter($"IsImp")
      .selectAs[FeatureBidsImpression]

    val convertedImpressions = parsedBidsImp.join(
      attributedEvents,
      Seq("AdGroupId", "CampaignId", "AdvertiserId", "BidRequestId"),
      "left_semi"
    ).selectAs[FeatureBidsImpression]

    val rows = ConvertedImpressionDataset(attLookback = convLookback).writePartition(convertedImpressions, date, Some(partCount.DailyConvertedImpressions))

    Array(rows)

  }
}
