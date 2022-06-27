package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.{loadParquetData, shiftModUdf}
import com.thetradedesk.geronimo.shared.schemas.{BidFeedbackDataset, BidFeedbackRecord}
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.TrainSetTransformation.TrackingTagWeightsRecord
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import job.GenerateTrainSet.modelDimensions
import job.OfflineScoringSetAttribution.{IsotonicRegNegSampleRate, IsotonicRegPositiveLabelCountThreshold}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{approx_count_distinct, broadcast, coalesce, col, floor, least, lit, percent_rank, row_number, sum, to_timestamp, when, xxhash64}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import java.time.format.DateTimeFormatter



object OfflineAttributionTransform {

  final case class OfflineScoreRecord(
                                     AdGroupId: String,
                                     BidRequestId: String,
                                     BidFeedbackId: String,
                                     Score: Double,
                                     Piece: Int,
                                     ClickRedirectId: Option[String]
  )

  final case class OfflineScoreAttributionsRecord(
                                                   AdGroupId: String,
                                                   BidFeedbackId: String,
                                                   Score: Double,
                                                   Piece:Int,
                                                   Label:Int,
                                                   AttributedEventTypeId: Option[String],
                                                   TrackingTagId: Option[String],
                                                   NormalizedPixelWeight: Double,
                                                   NormalizedCustomCPAClickWeight:Option[Double],
                                                   NormalizedCustomCPAViewthroughWeight:Option[Double],
                                                 )

  final case class OfflineScoreAttributionResultRecord(
                                                  AdGroupId: String,
                                                  BidFeedbackId: String,
                                                  Score: Double,
                                                  Piece:Int,
                                                  Label:Int,
                                                  PieceWeightedConversionRate:Double,
                                                  ImpressionWeightForCalibrationModel: Double
  )

  def getAttributedEventAndResult(
                                 endDate: java.time.LocalDate,
                                 lookBack: Int
                              )(implicit prometheus:PrometheusClient): Tuple2[Dataset[AttributedEventRecord], Dataset[AttributedEventResultRecord]] ={
    // 2022-04-27 ~ 2022-05-04
    val attributedEvent = loadParquetData[AttributedEventRecord](
      AttributedEventDataset.S3Path,
      date = endDate,
      lookBack = Some(lookBack),
      dateSeparator = Some("-")
    )

    // 2022-04-27 ~ 2022-05-04
    val attributedEventResult = loadParquetData[AttributedEventResultRecord](
      AttributedEventResultDataset.S3Path,
      date = endDate,
      lookBack = Some(lookBack),
      dateSeparator = Some("-")

    )

    (attributedEvent, attributedEventResult)

  }

  def getOfflineScore(
                       modelDate: java.time.LocalDate,
                       endDate: java.time.LocalDate,
                       lookBack: Int
                     )(implicit prometheus:PrometheusClient):Dataset[OfflineScoreRecord] ={

    // 1. load offline scores
      val multidayOfflineScore =  loadParquetData[OfflineScoredImpressionRecord](
        OfflineScoredImpressionDataset.S3BasePath+s"/model_date=${modelDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}"
        ,date = endDate
        ,lookBack = Some(lookBack)
        ,partitionPrefix = Some("scored_date")
      ).select($"BidRequestId", $"AdGroupId", $"Score")

    // todo: maybe we should add bidfeedbackid to bidimpression schema
    //  , and add bidbeedbackid to scoring dataset. So there's no need to join feedback again here.
    //2. load bidfeedback to join the bidfeedbackid to offline scores
    val bidfeedback = loadParquetData[BidFeedbackRecord](
      BidFeedbackDataset.BFS3,
      date = endDate,
      lookBack = Some(lookBack)
    )

    //3. load clicks
    val clicks = loadParquetData[ClickTrackerRecord](
      ClickTrackerDataset.S3Path,
      date = endDate,
      lookBack = Some(lookBack)
    )
  //can one impression click multiple times?

    val scoreQuantileWindow = Window.partitionBy($"AdGroupId").orderBy($"Score")

    multidayOfflineScore
      .join(bidfeedback, Seq("BidRequestId"))
      .withColumn("Piece", least(floor((percent_rank() over(scoreQuantileWindow))/lit(0.05)), lit(19) ).cast(IntegerType))
      .join(clicks, Seq("BidRequestId"),"left")
      .selectAs[OfflineScoreRecord]
  }

  def getOfflineScoreLabelWeight(
                          offlineScore: Dataset[OfflineScoreRecord],
                          attributedEvent: DataFrame,
                          attributedEventResult: DataFrame,
                          pixelWeight: Dataset[TrackingTagWeightsRecord]
                          )(implicit prometheus:PrometheusClient): Dataset[OfflineScoreAttributionsRecord] = {

    val conversionWindow =  Window.partitionBy($"ConversionTrackerId").orderBy($"AttributedEventLogEntryTime".desc)
    val attributedEventResultOfInterest = attributedEvent.join(attributedEventResult,
      Seq("ConversionTrackerLogFileId","ConversionTrackerIntId1","ConversionTrackerIntId2","AttributedEventLogFileId","AttributedEventIntId1","AttributedEventIntId2"),
      "inner")
      .join(pixelWeight.withColumnRenamed("ConfigValue", "AdGroupId"), Seq("AdGroupId", "TrackingTagId"), "inner")

    val offlineScoreAttributed =
      attributedEventResultOfInterest.filter($"AttributedEventTypeId"===lit("1")).join(offlineScore, col("AttributedEventId")===col("ClickRedirectId"), "inner")
      .union(attributedEventResultOfInterest.filter($"AttributedEventTypeId"===lit("2")).join(offlineScore, col("AttributedEventId")===col("BidFeedbackId"), "inner"))
      .withColumn("DedupRank",  row_number() over(conversionWindow))
      .filter($"DedupRank"===lit(1)).drop("DedupRank","AdGroupId","BidFeedbackId","Score","Piece","ClickRedirectId")

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

    val adgroupScorePieceWindow = Window.partitionBy($"AdGroupId", $"Piece")
    val impressionWindow = Window.partitionBy($"BidFeedbackId")

    offlineScoreLabel
      .withColumn("ConversionWeight",
        when($"NormalizedCustomCPAViewthroughWeight".isNotNull&&$"AttributedEventTypeId"===lit("2"), $"NormalizedCustomCPAViewthroughWeight")
          .when($"NormalizedCustomCPAClickWeight".isNotNull&&$"AttributedEventTypeId"===lit("1"), $"NormalizedCustomCPAClickWeight")
          .when($"AttributedEventTypeId".isNotNull, $"NormalizedPixelWeight").otherwise(lit(null))
      ).withColumn("PieceWeightedTotalConversion", sum(coalesce($"ConversionWeight", lit(0))).over(adgroupScorePieceWindow))
      .withColumn("PieceTotalImpression", approx_count_distinct($"BidFeedbackId").over(adgroupScorePieceWindow))
      .withColumn("PieceWeightedConversionRate", $"PieceWeightedTotalConversion"/$"PieceTotalImpression")
      .withColumn("ImpressionWeightForCalibrationModel", sum(coalesce($"ConversionWeight", lit(1))).over(impressionWindow))
      .select($"AdGroupId", $"BidFeedbackId", $"Piece", $"Label", $"Score", $"PieceWeightedConversionRate", $"ImpressionWeightForCalibrationModel")
      .distinct()
      .selectAs[OfflineScoreAttributionResultRecord]

  }

  def getInputForCalibrationAndBiasTuning(
                                           impressionLevelPerformance: Dataset[OfflineScoreAttributionResultRecord] ,
                                           totalImpressionsCount: Int,
                                           adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                         )(implicit prometheus:PrometheusClient): Tuple2[Dataset[ImpressionForIsotonicRegRecord], Dataset[AdGroupCvrForBiasTuningRecord]] = {

    val convertedImpressions = impressionLevelPerformance.filter($"Label"===lit(1)).cache()

    // 1. prepare adgroup data for isotonic regression
    // filter out adgroups impressions that has conversions more than threshold.
    val adGroupsHaveEnoughConversion = convertedImpressions
      .groupBy($"AdGroupId").count()
      .filter($"count">IsotonicRegPositiveLabelCountThreshold)
      .select($"AdGroupId").distinct()

    val feedToIsotonicRegressionImpressions = impressionLevelPerformance
      .join(
        adGroupsHaveEnoughConversion,
        Seq("AdGroupId"), "left_semi"
      )

    val adgroupIdCardinality = modelDimensions(0).cardinality.getOrElse(0)

    // downsample negative samples, reflect the sample rate to weight.
    val feedToIsotonicRegressionSampled = feedToIsotonicRegressionImpressions
      .filter($"Label"===lit(0))
      .sample(IsotonicRegNegSampleRate)
      .withColumn("ImpressionWeightForCalibrationModel", $"ImpressionWeightForCalibrationModel"/lit(IsotonicRegNegSampleRate))
      .union(
        feedToIsotonicRegressionImpressions.filter($"Label"===lit(1))
      )
      .withColumn("AdGroupIdInt", shiftModUdf(xxhash64(col("AdGroupId")), lit(adgroupIdCardinality)))
      .select($"AdGroupId".as("AdGroupIdStr"),$"AdgroupIdInt".as("AdgroupId"),$"PieceWeightedConversionRate",$"Score",$"Label",$"ImpressionWeightForCalibrationModel")
      .as[ImpressionForIsotonicRegRecord]

    // 2. prepare adgroup data for bias runing, every adgroup will have a conversion rate to be calcuated. In the calibration job if isotonic regression's quality isn't good, falls back to bias tuning.
    // default conversion rate for bias tuning
    val defaultCvr = convertedImpressions.agg(sum($"ImpressionWeightForCalibrationModel")).first().getDouble(0)/totalImpressionsCount

    // CVR: when $"TotalConversions"/$"TotalImpressions">0, meaning the ratio is neither null nor 0. It could be zero when impression label is 1, but  weight is set to zero by client
    // when $"TotalConversions"/$"TotalImpressions">1, usually there are too less impressions and some impression converted many times. It's better to fall back to default.
    val adgroupCVR = impressionLevelPerformance.groupBy("AdGroupId").count().withColumnRenamed("count","TotalImpressions")
      .join(
        convertedImpressions.groupBy("AdGroupId").agg(sum($"ImpressionWeightForCalibrationModel").as("TotalConversions")),
        Seq("AdGroupId"),
        "left"
      ).withColumn("CVR", when(($"TotalConversions"/$"TotalImpressions">0)&&($"TotalConversions"/$"TotalImpressions"<1), $"TotalConversions"/$"TotalImpressions").otherwise(lit(defaultCvr))
    ).select($"AdGroupId".as("AdGroupIdStr"),$"CVR")
      .cache()

    // ensure every adgroupid in the adgrouppolicy table has calibration even if they don't show up in score set.
    val feedToBiasTuningAdgroupCVR = adGroupPolicy.select($"ConfigValue".as("AdGroupIdStr")).join(
      adgroupCVR.select("AdGroupIdStr"), Seq("AdGroupIdStr"), "left_anti"
    )
      .withColumn("CVR", lit(defaultCvr))
      .union(adgroupCVR)
      .withColumn("AdGroupId",shiftModUdf(xxhash64(col("AdGroupIdStr")), lit(adgroupIdCardinality)))
      .union(Seq(("default_cvr", defaultCvr, -1)).toDF("AdGroupIdStr","CVR", "AdgroupId"))
      .as[AdGroupCvrForBiasTuningRecord]

    (feedToIsotonicRegressionSampled, feedToBiasTuningAdgroupCVR)
  }

}
