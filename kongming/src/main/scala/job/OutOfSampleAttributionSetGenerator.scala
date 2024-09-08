package job

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, seqDirectFields, seqHashFields}
import com.thetradedesk.kongming.transform.TrainSetTransformation.getValidTrackingTags
import com.thetradedesk.kongming.transform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object OutOfSampleAttributionSetGenerator extends KongmingBaseJob {

  override def jobName: String = "OutOfSampleAttributionSetGenerator"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val delayNDays = Config.ImpressionLookBack + Config.AttributionLookBack
    val scoreDate = date.minusDays(delayNDays)

    val attributionSet = generateAttributionSet(scoreDate)(getPrometheus)

    val OptInTrackedAttributionSet = attributionSet._1.filter($"IsTracked" === lit(1)).selectAs[OutOfSampleAttributionRecord]
    val OptInUntrackedAttributionSet = attributionSet._1.filter($"IsTracked" =!= lit(1)).selectAs[OutOfSampleAttributionRecord]

    val OptOutTrackedAttributionSet = attributionSet._2.filter($"IsTracked" === lit(1)).selectAs[OutOfSampleAttributionRecord]
    val OptOutUntrackedAttributionSet = attributionSet._2.filter($"IsTracked" =!= lit(1)).selectAs[OutOfSampleAttributionRecord]

    val TrackedAttributionSet = attributionSet._3.filter($"IsTracked" === lit(1)).selectAs[OldOutOfSampleAttributionRecord]
    val UntrackedAttributionSet = attributionSet._3.filter($"IsTracked" =!= lit(1)).selectAs[OldOutOfSampleAttributionRecord]



    val numTrackedRows = OutOfSampleAttributionDataset(delayNDays, userDataOptIn = 1).writePartition(OptInTrackedAttributionSet, scoreDate, "tracked", Some(1000))
    val numUntrackedRows = OutOfSampleAttributionDataset(delayNDays, userDataOptIn = 1).writePartition(OptInUntrackedAttributionSet, scoreDate, "untracked", Some(200))

    OutOfSampleAttributionDataset(delayNDays, userDataOptIn = 0).writePartition(OptOutTrackedAttributionSet, scoreDate, "tracked", Some(1000))
    OutOfSampleAttributionDataset(delayNDays, userDataOptIn = 0).writePartition(OptOutUntrackedAttributionSet, scoreDate, "untracked", Some(200))

    OldOutOfSampleAttributionDataset(delayNDays).writePartition(TrackedAttributionSet, scoreDate, "tracked", Some(200))
    OldOutOfSampleAttributionDataset(delayNDays).writePartition(UntrackedAttributionSet, scoreDate, "untracked", Some(200))

    Array(numTrackedRows, numUntrackedRows)

  }



  final case class AttributionBidFeedbackRecord(
                                                 BidRequestId: String,
                                                 BidFeedbackId: String,
                                                 AdGroupId: String,
                                                 CampaignId: String,
                                                 AdvertiserId: String
                                               )

  final case class AttributionEventRecordWithConfigKeys(
                                                         BidRequestId: String,
                                                         AttributedEventId: String,
                                                         ConfigKey: String,
                                                         ConfigValue: String
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

  final case class BidRequestsWithConfigValue(
                                                  BidRequestIdStr: String,
                                                  ConfigKey: String,
                                                  ConfigValue: String
                                                  )

  final case class BidRequestsWithAdGroupId(
                                               BidRequestIdStr: String,
                                               AdGroupId: String
                                             )

  final case class EventsAttribution(
                                      BidRequestId: String,
                                      Target: Int,
                                      Revenue: Option[BigDecimal]
                                    )

  def getEventsAttributions(adGroupPolicy: Dataset[AdGroupPolicyRecord], adGroupPolicyMapping: Dataset[AdGroupPolicyMappingRecord], scoreDate: LocalDate)(implicit prometheus: PrometheusClient): Dataset[EventsAttribution] = {
    val validTags = getValidTrackingTags(scoreDate, adGroupPolicy)

    val (attributedEvent, attributedEventResult) = OfflineAttributionTransform.getAttributedEventAndResult(adGroupPolicyMapping, date, lookBack = Config.AttributionLookBack + Config.ImpressionLookBack - 1)
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

    val bidFeedBack = multiLevelJoinWithPolicy[AttributionEventRecordWithConfigKeys](
      loadParquetData[AttributionBidFeedbackRecord](BidFeedbackDataset.BFS3, scoreDate.plusDays(Config.ImpressionLookBack), lookBack = Some(Config.ImpressionLookBack - 1))
        .withColumnRenamed("BidFeedbackId", "AttributedEventId"),
      adGroupPolicy, "inner")

    val clicks = multiLevelJoinWithPolicy[AttributionEventRecordWithConfigKeys](
      ClickTrackerDataSetV5(defaultCloudProvider).readRange(scoreDate.plusDays(1).atStartOfDay(), scoreDate.plusDays(Config.ImpressionLookBack + 1).atStartOfDay())
        .withColumnRenamed("ClickRedirectId", "AttributedEventId"),
      adGroupPolicy, "inner")

    val attribution = attributedEventResultOfInterest.filter($"AttributedEventTypeId" === lit("2")).join(bidFeedBack, Seq("AttributedEventId", "ConfigKey", "ConfigValue"), "inner")
      .union(attributedEventResultOfInterest.filter($"AttributedEventTypeId" === lit("1")).join(clicks, Seq("AttributedEventId", "ConfigKey", "ConfigValue"), "inner"))
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

  def generateAttributionSet(scoreDate: LocalDate)(implicit prometheus: PrometheusClient):
  (Dataset[OutOfSampleAttributionRecord], Dataset[OutOfSampleAttributionRecord], Dataset[OldOutOfSampleAttributionRecord]) = {
    val adGroupPolicy = AdGroupPolicyDataset().readDate(scoreDate)
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(scoreDate)
    val policy = getMinimalPolicy(adGroupPolicy, adGroupPolicyMapping).cache()

    val scoringSet = OldDailyOfflineScoringDataset().readRange(scoreDate.plusDays(1), scoreDate.plusDays(Config.ImpressionLookBack), isInclusive=true)

    val impressionsToScore = multiLevelJoinWithPolicy[BidRequestsWithAdGroupId](
      scoringSet.withColumnRenamed("AdGroupId", "AdGroupIdInt").withColumnRenamed("AdGroupIdStr", "AdGroupId")
        .withColumnRenamed("CampaignId", "CampaignIdInt").withColumnRenamed("CampaignIdStr", "CampaignId")
        .withColumnRenamed("AdvertiserId", "AdvertiserIdInt").withColumnRenamed("AdvertiserIdStr", "AdvertiserId"),
      policy,
      "inner")

    val eventsAttributions = getEventsAttributions(policy, adGroupPolicyMapping, scoreDate)(prometheus)
      .withColumnRenamed("BidRequestId", "BidRequestIdStr")

    // offline score labels: one impression could have more than one row if it contributes to multiple conversions. If it contributes to no conversion, then it has one row.
    val NegPosRatio = Config.OosNegPosRatio
    val win = Window.partitionBy("AdGroupId")
    val rawOOS = impressionsToScore.join(eventsAttributions, Seq("BidRequestIdStr"), "left")
      .withColumn("Target", coalesce('Target, lit(0)))
      .withColumn("Revenue", coalesce('Revenue, lit(0)))
      .withColumn("AdGroupPosCount", sum($"Target").over(win))
      .withColumn("AdGroupNegCount", sum(lit(1) - $"Target").over(win))
      .withColumn("SamplingRate", $"AdGroupPosCount" * lit(NegPosRatio) / $"AdGroupNegCount")
      .withColumn("SamplingRate", when($"Target" === lit(1), 1).otherwise($"SamplingRate"))
      .withColumn("rand", rand(seed = samplingSeed))
      .filter($"rand" <= $"SamplingRate")
      .drop("AdGroupId", "AdGroupPosCount", "AdGroupNegCount", "SamplingRate", "rand")
      .join(scoringSet, Seq("BidRequestIdStr"), "inner")
      .cache()

    val campaignsWithPosSamples = rawOOS.filter('Target === lit(1)).select('CampaignId).distinct
    val parquetSelectionTabular = rawOOS.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqDirectFields ++ seqHashFields)

    val rawOOSFiltered =  rawOOS.join(broadcast(campaignsWithPosSamples), Seq("CampaignId"), "left_semi")
      .select(parquetSelectionTabular: _*)
      .cache()

    val rawOOSUserDataOptIn = rawOOSFiltered
      .withColumn("UserDataOptIn",lit(2)) // hashmod 1 ->2
      .selectAs[OutOfSampleAttributionRecord]

    val rawOOSUserDataOptOut = rawOOSFiltered
      .withColumn("UserDataOptIn",lit(1)) // hashmod 0 ->1
      .selectAs[OutOfSampleAttributionRecord]

    val rawOOSdata = rawOOSFiltered.selectAs[OldOutOfSampleAttributionRecord]

    (rawOOSUserDataOptIn, rawOOSUserDataOptOut, rawOOSdata)
  }
}
