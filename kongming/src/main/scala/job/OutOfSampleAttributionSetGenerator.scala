package job

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, seqFields}
import com.thetradedesk.kongming.transform.TrainSetTransformation.getValidTrackingTags
import com.thetradedesk.kongming.transform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate

object OutOfSampleAttributionSetGenerator extends KongmingBaseJob {

  override def jobName: String = "OutOfSampleAttributionSetGenerator"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val scoreDate = date.minusDays(Config.ImpressionLookBack + Config.AttributionLookBack)

    val attributionSet = generateAttributionSet(scoreDate)(getPrometheus)
    val numRows = OutOfSampleAttributionDataset().writePartition(attributionSet, scoreDate, Some(200))

    Array(numRows)

  }

  object Config {
    val ImpressionLookBack = config.getInt("OutOfSampleAttributeSetGenerator.ImpressionLookBack", 3)
    val AttributionLookBack = config.getInt("OutOfSampleAttributeSetGenerator.AttributionLookBack", 7)
  }

  final case class AttributionBidFeedbackRecord(
                                                 BidRequestId: String,
                                                 BidFeedbackId: String,
                                                 AdGroupId: String,
                                                 CampaignId: String,
                                                 AdvertiserId: String
                                               )

  final case class AttributionClickTrackerRecord(
                                                BidRequestId: String,
                                                ClickRedirectId: String,
                                                AdGroupId: String,
                                                CampaignId: String,
                                                AdvertiserId: String
                                                )

  final case class AttributedEventRecordWithPolicyKeys(
                                                         AttributedEventId: String,
                                                         AttributedEventTypeId: String,
                                                         ConversionTrackerLogFileId: String,
                                                         ConversionTrackerIntId1: String,
                                                         ConversionTrackerIntId2: String,
                                                         AttributedEventLogFileId: String,
                                                         AttributedEventIntId1: String,
                                                         AttributedEventIntId2: String,
                                                         AttributedEventLogEntryTime: String,// is string in parquet
                                                         ConversionTrackerId: String,
                                                         TrackingTagId: String,
                                                         ConfigKey: String,
                                                         ConfigValue: String,
                                                         MonetaryValue: Option[String],
                                                         MonetaryValueCurrency: Option[String]
                                                       )

  final case class BidRequestsWithAttributionGroup(
                                                  BidRequestIdStr: String,
                                                  AttributionGroupKey: String,
                                                  AttributionGroupValue: String
                                                  )

  final case class EventsAttribution(
                                      BidRequestId: String,
                                      Target: Int,
                                      Revenue: Option[BigDecimal]
                                    )

  def getEventsAttributions(adGroupPolicy: Dataset[_], adGroups: Dataset[AdGroupRecord], scoreDate: LocalDate)(implicit prometheus: PrometheusClient): Dataset[EventsAttribution] = {
    val validTags = getValidTrackingTags(scoreDate, adGroupPolicy)

    val (attributedEvent, attributedEventResult) = OfflineAttributionTransform.getAttributedEventAndResult(adGroupPolicy.selectAs[AdGroupPolicyRecord], date, lookBack = Config.AttributionLookBack + Config.ImpressionLookBack - 1)
    val attributedEventResultOfInterest = multiLevelJoinWithPolicy[AttributedEventRecordWithPolicyKeys](attributedEvent, adGroupPolicy, "inner")
      .withColumn("MonetaryValue", $"MonetaryValue".cast("decimal"))
      .join(
        attributedEventResult.withColumn("ConversionTrackerLogEntryTime", to_timestamp(col("ConversionTrackerLogEntryTime")).as("ConversionTrackerLogEntryTime")),
        Seq("ConversionTrackerLogFileId", "ConversionTrackerIntId1", "ConversionTrackerIntId2", "AttributedEventLogFileId", "AttributedEventIntId1", "AttributedEventIntId2"),
        "inner")
      .join(
        validTags.withColumnRenamed("ReportingColumnId", "CampaignReportingColumnId"),
        Seq("ConfigKey", "ConfigValue", "TrackingTagId", "CampaignReportingColumnId"),
        "inner")
      .filter($"AttributedEventLogEntryTime".isNotNull && ((unix_timestamp($"ConversionTrackerLogEntryTime") - unix_timestamp($"AttributedEventLogEntryTime")) <= Config.AttributionLookBack*86400))

    val bidFeedBack = multiLevelJoinWithPolicy[AttributionBidFeedbackRecord](
      loadParquetData[AttributionBidFeedbackRecord](BidFeedbackDataset.BFS3, scoreDate.plusDays(Config.ImpressionLookBack), lookBack = Some(Config.ImpressionLookBack - 1)),
      adGroupPolicy, "left_semi", "AttributionGroupKey", "AttributionGroupValue")
      .select("BidFeedbackId", "BidRequestId")

    val clicks = multiLevelJoinWithPolicy[AttributionClickTrackerRecord](
        ClickTrackerDataSetV5(defaultCloudProvider).readRange(scoreDate.plusDays(1).atStartOfDay(), scoreDate.plusDays(Config.ImpressionLookBack + 1).atStartOfDay()),
        adGroupPolicy, "left_semi", "AttributionGroupKey", "AttributionGroupValue")
      .select("BidRequestId", "ClickRedirectId")

    val attribution = attributedEventResultOfInterest.filter($"AttributedEventTypeId" === lit("2")).join(bidFeedBack, col("AttributedEventId") === col("BidFeedbackId"), "inner")
      .union(attributedEventResultOfInterest.filter($"AttributedEventTypeId" === lit("1")).join(clicks, col("AttributedEventId") === col("ClickRedirectId"), "inner"))
      .select("BidRequestId", "MonetaryValue", "MonetaryValueCurrency")
      .distinct().withColumn("Target", lit(1))

      task match {
        case "roas" => {
          val rate = DailyExchangeRateDataset().readDate(scoreDate).cache() // use scoreDate's exchange to simplify the calculation
          attribution
            .withColumn("CurrencyCodeId", $"MonetaryValueCurrency").join(broadcast(rate), Seq("CurrencyCodeId"), "left")
            .withColumn("ValidRevenue", $"MonetaryValue".isNotNull && $"MonetaryValueCurrency".isNotNull)
            .withColumn("FromUSD", when($"MonetaryValueCurrency" === "USD", lit(1)).otherwise($"FromUSD"))
            .withColumn("RevenueInUSD", when($"ValidRevenue", $"MonetaryValue" / $"FromUSD").otherwise(0))
            .withColumn("Revenue", greatest($"RevenueInUSD", lit(1)))
            .selectAs[EventsAttribution]
        }
        case _ => attribution.withColumn("Revenue", lit(0)).selectAs[EventsAttribution]
      }

  }

  def generateAttributionSet(scoreDate: LocalDate)(implicit prometheus: PrometheusClient): Dataset[OutOfSampleAttributionRecord] = {
    val adGroups = AdGroupDataSet().readLatestPartitionUpTo(scoreDate, true)
    val adGroupPolicy = AdGroupPolicyDataset().readDate(scoreDate)
      .withColumn("AttributionGroupKey", when('DataAggKey === lit("AdvertiserId"), lit("CampaignId")).otherwise('DataAggKey))
      .join(adGroups.select("AdGroupId", "CampaignId"), col("AdGroupId") === col("ConfigValue"), "left")
      .withColumn("AttributionGroupValue", when('DataAggKey === lit("AdvertiserId"), 'CampaignId).otherwise('DataAggValue))
      .drop("AdGroupId", "CampaignId")

    val scoringSet = DailyOfflineScoringDataset().readRange(scoreDate.plusDays(1), scoreDate.plusDays(Config.ImpressionLookBack), isInclusive=true)
    val impressionsToScore = multiLevelJoinWithPolicy[BidRequestsWithAttributionGroup](
      scoringSet.withColumnRenamed("AdGroupId", "AdGroupIdInt").withColumnRenamed("AdGroupIdStr", "AdGroupId")
        .withColumnRenamed("CampaignId", "CampaignIdInt").withColumnRenamed("CampaignIdStr", "CampaignId")
        .withColumnRenamed("AdvertiserId", "AdvertiserIdInt").withColumnRenamed("AdvertiserIdStr", "AdvertiserId"),
      adGroupPolicy.select("AttributionGroupKey", "AttributionGroupValue"),
      "inner", "AttributionGroupKey", "AttributionGroupValue")

    val eventsAttributions = getEventsAttributions(adGroupPolicy, adGroups, scoreDate)(prometheus)
      .withColumnRenamed("BidRequestId", "BidRequestIdStr")

    // offline score labels: one impression could have more than one row if it contributes to multiple conversions. If it contributes to no conversion, then it has one row.
    val rawOOS = impressionsToScore.join(eventsAttributions, Seq("BidRequestIdStr"), "left")
      .withColumn("Target", coalesce('Target, lit(0)))
      .withColumn("Revenue", coalesce('Revenue, lit(0)))
      .join(scoringSet, Seq("BidRequestIdStr"), "inner")

    val parquetSelectionTabular = rawOOS.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqFields)

    rawOOS
      .select(parquetSelectionTabular: _*)
      .selectAs[OutOfSampleAttributionRecord]

  }
}
