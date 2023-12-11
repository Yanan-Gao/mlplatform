package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.streaming.records.rtb.bidfeedback.BidFeedbackRecord
import com.thetradedesk.geronimo.shared.schemas.BidFeedbackDataset
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.multiLevelJoinWithPolicy
import com.thetradedesk.kongming.transform.TrainSetTransformation.TrackingTagWeightsRecord
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.datalake.ClickTrackerDataSetV5
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.defaultCloudProvider
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._


object OfflineAttributionTransform {

  final case class OfflineScoreRecord(
                                     AdGroupId: String,
                                     CampaignId: String,
                                     BidRequestId: String,
                                     BidFeedbackId: String,
                                     Score: Double,
                                     ClickRedirectId: Option[String]
  )

  final case class OfflineScoreAttributionsRecord(
                                                   AdGroupId: String,
                                                   CampaignId: String,
                                                   BidFeedbackId: String,
                                                   Score: Double,
                                                   Label:Int,
                                                   AttributedEventTypeId: Option[String],
                                                   TrackingTagId: Option[String],
                                                   NormalizedPixelWeight: Double,
                                                   NormalizedCustomCPAClickWeight:Option[Double],
                                                   NormalizedCustomCPAViewthroughWeight:Option[Double],
                                                 )

  final case class OfflineScoreAttributionResultRecord(
                                                  AdGroupId: String,
                                                  CampaignId: String,
                                                  Score: Double,
                                                  Label:Int,
                                                  ImpressionWeightForCalibrationModel: Double
  )

  final case class OfflineScoreAttributionResultPerAdGroupRecord(
                                                        AdGroupId: String,
                                                        Score: Double,
                                                        Label:Int,
                                                        ImpressionWeightForCalibrationModel: Double
                                                      )

  final case class OfflineScoreAttributionResultPerCampaignRecord(
                                                                  CampaignId: String,
                                                                  Score: Double,
                                                                  Label:Int,
                                                                  ImpressionWeightForCalibrationModel: Double
                                                                )

  def getAttributedEventAndResult(
                                 adGroupPolicyMapping: Dataset[AdGroupPolicyMappingRecord],
                                 endDate: java.time.LocalDate,
                                 lookBack: Int
                              )(implicit prometheus:PrometheusClient): Tuple2[Dataset[AttributedEventRecord], Dataset[AttributedEventResultRecord]] ={
    // AttributedEventTypeId: 1->Click, 2-> Impression；
    // AttributionMethodId: 0 -> 'LastClick' , 1 -> 'ViewThrough' , 2 -> 'Touch', 3 -> 'Decay'
    val attributedEvent =  AttributedEventDataSet().readRange(endDate.minusDays(lookBack), endDate, isInclusive = true).selectAs[AttributedEventRecord]
    val filteredAttributedEvent = attributedEvent.join(adGroupPolicyMapping, Seq("AdGroupId"), "left_semi")
      .filter($"AttributedEventTypeId".isin(List("1", "2"): _*))
      .withColumn("AttributedEventLogEntryTime", to_timestamp(col("AttributedEventLogEntryTime")).as("AttributedEventLogEntryTime"))
      .selectAs[AttributedEventRecord]


    val attributedEventResult = AttributedEventResultDataSet().readRange(endDate.minusDays(lookBack), endDate, isInclusive = true)
      .filter($"AttributionMethodId".isin(List("0", "1"): _*))
      .selectAs[AttributedEventResultRecord]

    (filteredAttributedEvent, attributedEventResult)

  }

  def getOfflineScore(
                       modelDate: java.time.LocalDate,
                       endDate: java.time.LocalDate,
                       lookBack: Int,
                       adgroupBaseAssociateMapping: Dataset[AdGroupPolicyMappingRecord]
                     )(implicit prometheus:PrometheusClient):Dataset[OfflineScoreRecord] ={

    val adgroupIdScored = adgroupBaseAssociateMapping.select("AdGroupId").distinct().cache()

    // 1. load offline scores
    val multidayOfflineScore = OfflineScoredImpressionDataset(modelDate)
      .readRange(endDate.minusDays(lookBack), endDate, true)
      .select($"BidRequestId", $"BaseAdGroupId", $"AdGroupId", $"Score")


    // todo: maybe we should add bidfeedbackid to bidimpression schema
    //  , and add bidbeedbackid to scoring dataset. So there's no need to join feedback again here.
    //2. load bidfeedback to join the bidfeedbackid to offline scores
    val bidfeedback = loadParquetData[BidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = endDate,
      lookBack = Some(lookBack)
    ).join(broadcast(adgroupIdScored), Seq("AdGroupId"), "left_semi")
      .selectAs[BidFeedbackRecord]

    //3. load clicks
    val clicks = ClickTrackerDataSetV5(defaultCloudProvider)
      .readRange(endDate.minusDays(lookBack).atStartOfDay(), endDate.plusDays(1).atStartOfDay())
      .join(broadcast(adgroupIdScored), Seq("AdGroupId"), "left_semi")
      .select($"BidRequestId", $"ClickRedirectId")
  //can one impression click multiple times?

    multidayOfflineScore
      .join(bidfeedback, Seq("BidRequestId","AdGroupId"))
      .join(clicks, Seq("BidRequestId"),"left")
      .selectAs[OfflineScoreRecord]
  }

  def getOfflineScoreLabelWeight(
                          offlineScore: Dataset[OfflineScoreRecord],
                          attributedEvent: Dataset[AttributedEventRecord],
                          attributedEventResult: Dataset[AttributedEventResultRecord],
                          pixelWeight: Dataset[TrackingTagWeightsRecord]
                          )(implicit prometheus:PrometheusClient): Dataset[OfflineScoreAttributionsRecord] = {

    val attributedEventResultOfInterest = attributedEvent
      .join(attributedEventResult,
        Seq("ConversionTrackerLogFileId","ConversionTrackerIntId1","ConversionTrackerIntId2","AttributedEventLogFileId","AttributedEventIntId1","AttributedEventIntId2"),
        "inner"
      )
      .join(
        pixelWeight
          .withColumnRenamed("ConfigValue", "AdGroupId")
          .withColumnRenamed("ReportingColumnId", "CampaignReportingColumnId"),
        Seq("AdGroupId", "TrackingTagId","CampaignReportingColumnId"), "inner"
      )

    val offlineScoreAttributed =
      attributedEventResultOfInterest.filter($"AttributedEventTypeId"===lit("1")).join(offlineScore, col("AttributedEventId")===col("ClickRedirectId"), "inner")
      .union(attributedEventResultOfInterest.filter($"AttributedEventTypeId"===lit("2")).join(offlineScore, col("AttributedEventId")===col("BidFeedbackId"), "inner"))
      .drop("AdGroupId","CampaignId","BidFeedbackId","Score","ClickRedirectId")

    // offline score labels: one impression could have more than one row if it contributes to multiple conversions. If it contributes to no conversion, then it has one row.
    offlineScore.join(
      offlineScoreAttributed,
      Seq("BidRequestId"),
      "left"
    ).withColumn("Label", when($"AttributedEventLogEntryTime".isNotNull, lit(1)).otherwise(lit(0)))
      .selectAs[OfflineScoreAttributionsRecord]
  }

  def getOfflineScoreImpressionAndPiecePerformance(
                                                    offlineScoreLabel: Dataset[OfflineScoreAttributionsRecord]
                                                  )(implicit prometheus:PrometheusClient): Dataset[OfflineScoreAttributionResultRecord] = {

    val impressionWindow = Window.partitionBy($"BidFeedbackId")

    offlineScoreLabel
      .withColumn("ConversionWeight",
        when($"NormalizedCustomCPAViewthroughWeight".isNotNull&&$"AttributedEventTypeId"===lit("2"), $"NormalizedCustomCPAViewthroughWeight")
          .when($"NormalizedCustomCPAClickWeight".isNotNull&&$"AttributedEventTypeId"===lit("1"), $"NormalizedCustomCPAClickWeight")
          .when($"AttributedEventTypeId".isNotNull, $"NormalizedPixelWeight").otherwise(lit(null))
      )
      .withColumn("ImpressionWeightForCalibrationModel", sum(coalesce($"ConversionWeight", lit(1))).over(impressionWindow))
      .select($"AdGroupId", $"CampaignId", $"BidFeedbackId", $"Label", $"Score", $"ImpressionWeightForCalibrationModel")
      .distinct()
      .selectAs[OfflineScoreAttributionResultRecord]

  }

  def getInputForCalibrationAndScaling(
                                       impressionLevelPerformance: Dataset[OfflineScoreAttributionResultRecord] ,
                                       IsotonicRegPositiveLabelCountThreshold: Int,
                                       IsotonicRegNegCap: Int,
                                       IsotonicRegNegMaxSampleRate: Double,
                                       samplingSeed:Long
                                      )(implicit prometheus:PrometheusClient): (Dataset[ImpressionForIsotonicRegRecord], Dataset[CampaignCvrForScalingRecord]) = {
    val convertedImpressions = impressionLevelPerformance.filter($"Label"===lit(1)).filter($"ImpressionWeightForCalibrationModel">0).cache()

    // 1. prepare adgroup data for isotonic regression
    // filter in adgroups impressions that has conversions more than threshold.
    val adGroupsHaveEnoughConversion = convertedImpressions
      .groupBy($"AdGroupId").count()
      .filter($"count">IsotonicRegPositiveLabelCountThreshold)
      .select($"AdGroupId").distinct()

    val feedToIsotonicRegressionImpressions = impressionLevelPerformance
      .join(
        adGroupsHaveEnoughConversion,
        Seq("AdGroupId"), "left_semi"
      ).cache()

    // subsample negatives by cap
    val feedToIsotonicRegressionImpressionsNeg = feedToIsotonicRegressionImpressions.filter($"Label"===lit(0)).cache()
    val negCount = feedToIsotonicRegressionImpressionsNeg.groupBy($"AdGroupId").count()
      .withColumn("NegSampleRate", least(lit(IsotonicRegNegCap)/col("count"), lit(IsotonicRegNegMaxSampleRate)))
    val feedToIsotonicRegressionImpressionsNegSampled = feedToIsotonicRegressionImpressionsNeg.join(negCount, Seq("AdGroupId"), "inner")
      .filter(rand(seed=samplingSeed)<col("NegSampleRate"))
      .withColumn("ImpressionWeightForCalibrationModel", $"ImpressionWeightForCalibrationModel"/$"NegSampleRate")
      .selectAs[OfflineScoreAttributionResultPerAdGroupRecord]

    // union negative and positive
    val feedToIsotonicRegressionSampled = feedToIsotonicRegressionImpressionsNegSampled.union(
        feedToIsotonicRegressionImpressions.filter($"Label"===lit(1)).selectAs[OfflineScoreAttributionResultPerAdGroupRecord]
      )
      .select($"AdGroupId".as("Id"),$"Score",$"Label",$"ImpressionWeightForCalibrationModel")
      .withColumn("Level", lit("AdGroup"))
      .as[ImpressionForIsotonicRegRecord]

    // 2. prepare campaign data for isotonic regression.
    val campaignsHaveEnoughConversion = convertedImpressions
      .groupBy($"CampaignId").count()
      .filter($"count">IsotonicRegPositiveLabelCountThreshold*2)
      .select($"CampaignId").distinct()

    val feedToCampaignIsotonicRegressionImpressions = impressionLevelPerformance
      .join(
        campaignsHaveEnoughConversion,
        Seq("CampaignId"), "left_semi"
      ).cache()

    // subsample negatives by cap
    val feedToCampaignIsotonicRegressionImpressionsNeg = feedToCampaignIsotonicRegressionImpressions.filter($"Label"===lit(0)).cache()
    val negCountCampaign = feedToCampaignIsotonicRegressionImpressionsNeg.groupBy($"CampaignId").count()
      .withColumn("NegSampleRate", least(lit(IsotonicRegNegCap*2)/col("count"), lit(IsotonicRegNegMaxSampleRate)))
    val feedToCampaignIsotonicRegressionImpressionsNegSampled = feedToCampaignIsotonicRegressionImpressionsNeg.join(negCountCampaign, Seq("CampaignId"), "inner")
      .filter(rand(seed=samplingSeed)<col("NegSampleRate"))
      .withColumn("ImpressionWeightForCalibrationModel", $"ImpressionWeightForCalibrationModel"/$"NegSampleRate")
      .selectAs[OfflineScoreAttributionResultPerCampaignRecord]

    // union negative and positive
    val feedToCampaignIsotonicRegressionSampled = feedToCampaignIsotonicRegressionImpressionsNegSampled.union(
      feedToCampaignIsotonicRegressionImpressions.filter($"Label"===lit(1)).selectAs[OfflineScoreAttributionResultPerCampaignRecord]
    )
      .select($"CampaignId".as("Id"),$"Score",$"Label",$"ImpressionWeightForCalibrationModel")
      .withColumn("Level", lit("Campaign"))
      .as[ImpressionForIsotonicRegRecord]
    // 3. prepare campaign data for scaled cvr, every campaign will have a conversion rate to be calculated. In the calibration job if isotonic regression's quality isn't good, falls back to scaled cvr

    // CVR: when $"TotalConversions"/$"TotalImpressions">0, meaning the ratio is neither null nor 0. It could be zero when impression label is 1, but  weight is set to zero by client
    // when $"TotalConversions"/$"TotalImpressions">1, usually there are too less impressions and some impression converted many times. It's better to fall back to default.
    val campaignCVR = impressionLevelPerformance.groupBy("CampaignId").count().withColumnRenamed("count","TotalImpressions")
      .join(
        convertedImpressions.groupBy("CampaignId").agg(sum($"ImpressionWeightForCalibrationModel").as("TotalConversions")),
        Seq("CampaignId"),
        "inner"
      )
      .withColumn("CVR", $"TotalConversions"/$"TotalImpressions")
      .filter(($"CVR">0)&&($"CVR"<1))
      .select($"CampaignId".as("CampaignIdStr"),$"CVR")
      .selectAs[CampaignCvrForScalingRecord]


    (feedToIsotonicRegressionSampled.union(feedToCampaignIsotonicRegressionSampled).selectAs[ImpressionForIsotonicRegRecord], campaignCVR)
  }

}
