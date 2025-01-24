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

import java.time.LocalDate


object OfflineAttributionTransform {

  final case class OfflineScoreRecord(
                                     AdGroupId: String,
                                     CampaignId: String,
                                     BidRequestId: String,
                                     BidFeedbackId: String,
                                     Score: Array[Double],
                                     ClickRedirectId: Option[String]
  )

  final case class OfflineScoreAttributionsRecord(
                                                   AdGroupId: String,
                                                   CampaignId: String,
                                                   BidFeedbackId: String,
                                                   Score: Array[Double],
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
                                                  Score: Array[Double],
                                                  Label:Int,
                                                  ImpressionWeightForCalibrationModel: Double
  )


  final case class PixelMappingRecord(
                                CampaignId: String,
                                TrackingTagId: String,
                                )


  def getPixelMappings(
                      date: LocalDate,
                      with_graph_info: Boolean = false,
                      ): Dataset[PixelMappingRecord] = {
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, isInclusive = true)
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, isInclusive = true)
    var ccrcProcessed = ccrc.join(
        broadcast(campaignDS.select($"CampaignId", $"CustomCPATypeId", $"CustomCPAClickWeight", $"CustomCPAViewthroughWeight")),
        Seq("CampaignId"), "left"
      ).filter(($"CustomCPATypeId" === 0 && $"ReportingColumnId" === 1) || ($"CustomCPATypeId" > 0 && $"IncludeInCustomCPA"))

    if (with_graph_info) {
      ccrcProcessed = ccrcProcessed
        //  only use IAv2 and IAv2HH, other graphs will be replaced by IAv2
        .withColumn("CrossDeviceAttributionModelId",
          when(
            ($"CrossDeviceAttributionModelId".isNotNull) && !($"CrossDeviceAttributionModelId".isin(List("IdentityAllianceWithHousehold", "IdentityAlliance"): _*)),
            lit("IdentityAlliance")
          ).otherwise($"CrossDeviceAttributionModelId"))
          .withColumn("TrackingTagId", concat(
          col("TrackingTagId"),
          lit("_"),
          coalesce(col("CrossDeviceAttributionModelId"), lit("0"))
          )
        )
    }
    ccrcProcessed.selectAs[PixelMappingRecord]
  }


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

  private def getImpressionForIsotonic(
                              impressionLevelPerformance: Dataset[_],
                              level: String,
                              isotonicRegPositiveThreshold: Int,
                              isotonicRegNegativeCap: Int,
                              isotonicRegMaxSampleRate: Double,
                              samplingSeed: Long
                     ): Dataset[ImpressionForIsotonicRegRecord] = {

    val convertedImpressions = impressionLevelPerformance.filter(($"Label"===lit(1)) && ($"ImpressionWeightForCalibrationModel">0)).cache()

    // filter impressions that has conversions more than threshold.
    val idsToKeep = convertedImpressions.groupBy(col(level)).count()
      .filter($"count" > isotonicRegPositiveThreshold)
      .select(level).distinct()

    // keep ids that has enough conversions
    val feedToIsotonicRegressionImpressions = impressionLevelPerformance.join(
      idsToKeep, Seq(level), "left_semi"
    ).cache()

    // subsample negatives by cap
    val feedToIsotonicRegressionImpressionsNeg = feedToIsotonicRegressionImpressions.filter($"Label" === lit(0)).cache()
    val negCount = feedToIsotonicRegressionImpressionsNeg.groupBy(col(level)).count()
      .withColumn("NegSampleRate", least(lit(isotonicRegNegativeCap) / col("count"), lit(isotonicRegMaxSampleRate)))
    val feedToIsotonicRegressionImpressionsNegSampled = feedToIsotonicRegressionImpressionsNeg.join(
        negCount, Seq(level), "inner"
      ).filter(rand(seed = samplingSeed) < col("NegSampleRate"))
      .withColumn("ImpressionWeightForCalibrationModel", $"ImpressionWeightForCalibrationModel" / $"NegSampleRate")
      .select(level, "Score", "Label", "ImpressionWeightForCalibrationModel")

    // union negative and positive
    feedToIsotonicRegressionImpressionsNegSampled.union(
        feedToIsotonicRegressionImpressions.filter($"Label" === lit(1))
          .select(level, "Score", "Label", "ImpressionWeightForCalibrationModel")
      ).select(col(level).as("Id"), $"Score", $"Label", $"ImpressionWeightForCalibrationModel")
      .withColumn("Level", lit(level))
      .as[ImpressionForIsotonicRegRecord]
  }


  def getInputForCalibrationAndScaling(
                                           impressionLevelPerformance: Dataset[OfflineScoreAttributionResultRecord],
                                           pixelMappings: Dataset[PixelMappingRecord],
                                           IsotonicRegPositiveLabelCountThreshold: Int,
                                           IsotonicRegNegCap: Int,
                                           IsotonicRegNegMaxSampleRate: Double,
                                           samplingSeed:Long
                                         )(implicit prometheus:PrometheusClient): (Dataset[ImpressionForIsotonicRegRecord], Dataset[CvrForScalingRecord]) = {

    // 1. prepare impression data for isotonic regression in adgroup, campaign and pixel level
    val adGroupIsotonicRegSampled = getImpressionForIsotonic(
      impressionLevelPerformance,
      level = "AdGroupId",
      isotonicRegPositiveThreshold = IsotonicRegPositiveLabelCountThreshold,
      isotonicRegNegativeCap = IsotonicRegNegCap,
      isotonicRegMaxSampleRate = IsotonicRegNegMaxSampleRate,
      samplingSeed = samplingSeed
    )
    val campaignIsotonicRegSampled = getImpressionForIsotonic(
      impressionLevelPerformance,
      level = "CampaignId",
      isotonicRegPositiveThreshold = IsotonicRegPositiveLabelCountThreshold * 2,
      isotonicRegNegativeCap = IsotonicRegNegCap * 2,
      isotonicRegMaxSampleRate = IsotonicRegNegMaxSampleRate,
      samplingSeed = samplingSeed
    ).cache()

    // pixel level data is based on sampled campaign data. each pixel uses impressions of all campaigns it links to
    val pixelIsotonicRegSampled = getImpressionForIsotonic(
      impressionLevelPerformance.join(
        broadcast(pixelMappings), Seq("CampaignId"), "inner"
      ),
      level = "TrackingTagId",
      isotonicRegPositiveThreshold = IsotonicRegPositiveLabelCountThreshold * 2,
      isotonicRegNegativeCap = IsotonicRegNegCap * 2,
      isotonicRegMaxSampleRate = IsotonicRegNegMaxSampleRate,
      samplingSeed = samplingSeed
    )

    val impressionIsoRegSampled = adGroupIsotonicRegSampled.union(campaignIsotonicRegSampled).union(pixelIsotonicRegSampled).selectAs[ImpressionForIsotonicRegRecord]

    // 2. prepare cvr data for rescaling in campaign and pixel level
    // CVR corner cases:
    // when $"TotalConversions"/$"TotalImpressions">0, meaning the ratio is neither null nor 0. It could be zero when impression label is 1, but weight is set to zero by client
    // when $"TotalConversions"/$"TotalImpressions">1, usually there are too less impressions and some impression converted many times. It's better to fall back to default.
    val campaignCVR = impressionLevelPerformance.groupBy("CampaignId").count()
      .withColumnRenamed("count","TotalImpressions")
      .join(
        impressionLevelPerformance.filter(
          ($"Label"===lit(1)) && ($"ImpressionWeightForCalibrationModel">0)
        ).groupBy("CampaignId").agg(sum($"ImpressionWeightForCalibrationModel").as("TotalConversions")),
        Seq("CampaignId"),
        "inner"
      )
      .withColumn("CVR", $"TotalConversions"/$"TotalImpressions")

    // pixel CVR is calculated based on all campaigns it links to
    val pixelCVR = campaignCVR.join(pixelMappings, Seq("CampaignId"), "inner")
      .groupBy("TrackingTagId").agg(
        sum($"TotalConversions").as("TotalConversions"), sum($"TotalImpressions").as("TotalImpressions")
      ).withColumn("CVR", $"TotalConversions" / $"TotalImpressions")
      .withColumn("Level", lit("TrackingTagId"))
      .withColumnRenamed("TrackingTagId", "Id")
      .selectAs[CvrForScalingRecord]

    val scalingCVR = campaignCVR.withColumn("Level", lit("CampaignId"))
      .withColumnRenamed("CampaignId", "Id").selectAs[CvrForScalingRecord]
      .union(pixelCVR)
      .filter(($"CVR">0)&&($"CVR"<1))
      .selectAs[CvrForScalingRecord]

    (impressionIsoRegSampled, scalingCVR)
  }

}
