package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{ARRAY_INT_FEATURE_TYPE, FLOAT_FEATURE_TYPE, GERONIMO_DATA_SOURCE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, loadParquetData, shiftModArray, shiftModUdf}
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.ContextualTransform.generateContextualFeatureTier1
import com.thetradedesk.kongming.transform.AudienceIdTransform.AudienceFeature
import com.thetradedesk.kongming.{CustomGoalTypeId, IncludeInCustomGoal, date, task}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Encoder

import java.time.LocalDate

object TrainSetTransformation {
  case class UpSamplingPosFractionRecord(
                                          ConfigKey: String,
                                          ConfigValue: String,
                                          UpSamplingPosFraction: Double,
                                        )

  case class UpSamplingNegFractionRecord(
                                          ConfigKey: String,
                                          ConfigValue: String,
                                          UpSamplingNegFraction: Double,
                                        )

  case class PreFeatureJoinRecord(
                                   ConfigKey: String,
                                   ConfigValue: String,
                                   BidRequestId: String,
                                   Weight: Double,
                                   LogEntryTime:  java.sql.Timestamp,
                                   Target: Int,
                                   Revenue: Option[BigDecimal],
                                   IsInTrainSet: Boolean
                                 )

  case class PreFeatureJoinRecordV2(
                                   CampaignIdStr: String,
                                   CampaignIdEncoded: Long,
                                   BidRequestIdStr: String,
                                   ConversionTrackerLogEntryTime: String,
                                   LogEntryTimeStr: String,
                                   CPACountWeight: Double,
                                   Target: Int,
                                   Weight: Double
                                 )

  case class TrainSetRecord(
                             ConfigKey: String,
                             ConfigValue: String,
                             BidRequestId: String,
                             Revenue: Option[BigDecimal],
                             Weight: Double,
                             LogEntryTime:  java.sql.Timestamp,
                             IsInTrainSet: Boolean
                           )

  case class TrainSetRecordVerbose(
                                     ConfigKey: String,
                                     ConfigValue: String,
                                     BidRequestId: String,
                                     TrackingTagId: String,
                                     Revenue: Option[BigDecimal],
                                     Weight: Double,
                                     ConversionTime: java.sql.Timestamp,
                                     LogEntryTime: java.sql.Timestamp,
                                     IsInTrainSet: Boolean,

                                     IsClickWindowGreater: Boolean,
                                     IsInClickAttributionWindow: Boolean,
                                     IsInViewAttributionWindow: Boolean
                                  )

  case class TrackingTagWeightsRecord(
                                       TrackingTagId: String,
                                       ReportingColumnId: Int,
                                       ConfigKey: String,
                                       ConfigValue: String,
                                       NormalizedPixelWeight: Double,
                                       NormalizedCustomCPAClickWeight: Option[Double],
                                       NormalizedCustomCPAViewthroughWeight: Option[Double]
                                     )

  case class PositiveWithRawWeightsRecord(
                                           ConfigKey: String,
                                           ConfigValue: String,

                                           BidRequestId: String,

                                           IsClickWindowGreater: Boolean,
                                           IsInViewAttributionWindow: Boolean,
                                           IsInClickAttributionWindow: Boolean,

                                           NormalizedPixelWeight: Double,
                                           NormalizedCustomCPAClickWeight: Option[Double],
                                           NormalizedCustomCPAViewthroughWeight: Option[Double],
                                           ConversionTime: java.sql.Timestamp,
                                           LogEntryTime:  java.sql.Timestamp,
                                           IsInTrainSet: Boolean
                                         )

  case class NegativeWeightDistParams(
                                       Coefficient: Double,
                                       Offset: Double,
                                       Threshold: Int
                                     )

  case class TrackingTagRecord(
                                TrackingTagId: String,
                                ReportingColumnId: Int,
                                ConfigKey: String,
                                ConfigValue: String,
                              )

  def getValidTrackingTags(
                            endDate: LocalDate,
                            adGroupPolicy: Dataset[_],
                            // adGroupDS: Dataset[AdGroupRecord],
                          ): Dataset[TrackingTagRecord] = {
    val customGoalTypeId = CustomGoalTypeId.get(task).get
    val includeInCustomGoal = IncludeInCustomGoal.get(task).get

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(endDate, isInclusive = true)
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(endDate, true)
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(endDate, true)

    val ccrcPreProcessed = ccrc
      .join(broadcast(campaignDS.select($"CampaignId", col(customGoalTypeId), $"CustomCPAClickWeight", $"CustomCPAViewthroughWeight")), Seq("CampaignId"), "left")

    val ccrcProcessed = task match {
      case "roas" => {
        ccrcPreProcessed.filter(col(customGoalTypeId) === 0 && $"ReportingColumnId" === 1).union(
          ccrcPreProcessed.filter(col(customGoalTypeId) > 0 && col(includeInCustomGoal))
            .filter((col(customGoalTypeId) === lit(1)) && ($"CustomROASWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(2)) && ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight" =!= lit(0))
              or (col(customGoalTypeId) === lit(3)) && ($"CustomROASWeight" * ($"CustomROASClickWeight" + $"CustomROASViewthroughWeight") =!= lit(0))))
      }
      case _ => {
        ccrcPreProcessed.filter(col(customGoalTypeId) === 0 && $"ReportingColumnId" === 1).union(
          ccrcPreProcessed.filter(col(customGoalTypeId) > 0 && col(includeInCustomGoal))
            .filter((col(customGoalTypeId) === lit(1)) && ($"Weight" =!= lit(0))
              or (col(customGoalTypeId) === lit(2)) && ($"CustomCPAClickWeight" + $"CustomCPAViewthroughWeight" =!= lit(0)))
        )
      }
    }

    adGroupPolicy
      .join(broadcast(adGroupDS), adGroupPolicy("ConfigValue") === adGroupDS("AdGroupId"), "inner")
      .select("CampaignId", "ConfigKey", "ConfigValue", "DataAggKey", "DataAggValue")
      .join(ccrcProcessed, Seq("CampaignId"), "inner")
      .select($"TrackingTagId", $"ReportingColumnId", $"ConfigKey", $"ConfigValue")
      .selectAs[TrackingTagRecord]
  }

  def getWeightsForTrackingTags(
                                 date: LocalDate,
                                 adgroupBaseAssociateMapping: Dataset[AdGroupPolicyMappingRecord],
                                 normalized: Boolean=false,
                               ): Dataset[TrackingTagWeightsRecord]= {
    // 1. get the latest weights per campaign and trackingtagid
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, true)
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)
    val ccrcWindow = Window.partitionBy($"CampaignId")

    val ccrcProcessed = ccrc
      .join(broadcast(campaignDS.select($"CampaignId", $"CustomCPATypeId", $"CustomCPAClickWeight", $"CustomCPAViewthroughWeight")), Seq("CampaignId"), "left")
      .filter(($"CustomCPATypeId"===0 && $"ReportingColumnId"===1) || ($"CustomCPATypeId">0 && $"IncludeInCustomCPA") )
      .withColumn("TotalWeight", sum(coalesce($"Weight", lit(0))).over(ccrcWindow))
      .withColumn("NumberOfConversionTrackers", count($"TrackingTagId").over(ccrcWindow))
      .withColumn("NormalizedPixelWeight",
        when(lit(normalized)===lit(true),
          when($"TotalWeight">0,$"Weight"/$"TotalWeight" )
          .otherwise(lit(1)/$"NumberOfConversionTrackers")
        )
          .otherwise(
            when($"TotalWeight">0,$"Weight" )
              .otherwise(lit(1))
          )
      )
      .withColumn("NormalizedCustomCPAClickWeight",
        when(lit(normalized)===lit(true),
          when($"CustomCPATypeId"===2, coalesce($"CustomCPAClickWeight", lit(0)) /(coalesce($"CustomCPAClickWeight", lit(0))+coalesce($"CustomCPAViewthroughWeight", lit(0))))
            .otherwise(null)).otherwise(
          when($"CustomCPATypeId"===2, coalesce($"CustomCPAClickWeight", lit(0)))
            .otherwise(null)
        )
      )
      .withColumn("NormalizedCustomCPAViewthroughWeight",
        when(lit(normalized)===lit(true),
          when($"CustomCPATypeId"===2, coalesce($"CustomCPAViewthroughWeight", lit(0)) /(coalesce($"CustomCPAClickWeight", lit(0))+coalesce($"CustomCPAViewthroughWeight", lit(0))))
            .otherwise(null)).otherwise(
          when($"CustomCPATypeId"===2, coalesce($"CustomCPAViewthroughWeight", lit(0)))
            .otherwise(null)
        )
      )
      .withColumn("isClickViewWeightEmpty", $"NormalizedCustomCPAClickWeight".isNotNull && $"NormalizedCustomCPAViewthroughWeight".isNotNull &&(($"NormalizedCustomCPAClickWeight"+$"NormalizedCustomCPAViewthroughWeight")<lit(1e-8)))
      .withColumn("NormalizedCustomCPAClickWeight",
        when($"isClickViewWeightEmpty", lit(0.5)).otherwise($"NormalizedCustomCPAClickWeight"))
      .withColumn("NormalizedCustomCPAViewthroughWeight",
        when($"isClickViewWeightEmpty", lit(0.5)).otherwise($"NormalizedCustomCPAViewthroughWeight"))
      .select($"CampaignId",$"TrackingTagId", $"ReportingColumnId", $"NormalizedPixelWeight".cast(DoubleType),$"NormalizedCustomCPAClickWeight".cast(DoubleType), $"NormalizedCustomCPAViewthroughWeight".cast(DoubleType) )
    //isClickViewWeightEmpty: This is for handling the case where the customCPAType=2 so there should be a at least one non-zero weight for click or view,
    // but some campaigns didn't fill any of those two weights, in which case, our logic at line 145 and 153 sets both weights to 0, but not null value.
    //Besides, the zero weight is represented to 0e18, so I use clickweight+viewweight <1e-8 instead of clickweight+viewweight==0.

    // 2. join trackingtag weights with policy table
    adgroupBaseAssociateMapping
      .select("AdGroupId","CampaignId")
      .join(ccrcProcessed, Seq("CampaignId"), "inner")
      .select($"AdGroupId",$"TrackingTagId", $"ReportingColumnId", $"NormalizedPixelWeight",$"NormalizedCustomCPAClickWeight", $"NormalizedCustomCPAViewthroughWeight" ) // one trackingtagid can have different following values
      .withColumn("ConfigKey", lit("AdGroupId"))
      .withColumnRenamed("AdGroupId", "ConfigValue")
      .selectAs[TrackingTagWeightsRecord]
    // renaming AdGroupId to ConfigValue here only serve for matching the schema. AdGroupId in this table is not limited to ConfigValues in policy table
  }

  def generateWeightForNegativeV2[T: Encoder](
                                 negativeSamples: Dataset[T],
                                 positiveSamples: Dataset[T],
                                 lookBackDays: Int,
                                 weightMethod: Option[String] = None,
                                 methodDistParams: NegativeWeightDistParams,
                               ): Dataset[T] = {

    val adGroupDailyConvDist = positiveSamples
      .withColumn("BidDiffDayInt", floor((unix_timestamp($"ConversionTrackerLogEntryTime") - unix_timestamp($"LogEntryTimeStr","yyyy-MM-dd HH:mm:ss")) / 86400).cast("int"))
      .groupBy("CampaignIdEncoded", "BidDiffDayInt").agg(count($"BidRequestIdStr").alias("PosDailyCnt"))

    val dayRange = (0 to lookBackDays).toList
    val adGroupRangeDF = adGroupDailyConvDist.select("CampaignIdEncoded").distinct()
      .withColumn("DateRange", typedLit(dayRange))
      .withColumn("BidDiffDayInt", explode($"DateRange"))
      .drop("DateRange")
    val adGroupRangeConvDist = adGroupRangeDF
      .join(adGroupDailyConvDist, Seq("CampaignIdEncoded", "BidDiffDayInt"), "left")
      .withColumn("PosDailyCnt", coalesce('PosDailyCnt, lit(0)))
      .withColumn("CampaignGroupSize", sum("PosDailyCnt").over(Window.partitionBy("CampaignIdEncoded")))
      .withColumn("PosPctInNDay", sum("PosDailyCnt").over(Window.partitionBy("CampaignIdEncoded").orderBy("BidDiffDayInt"))
        / $"CampaignGroupSize")
    val negativeWithAdjustedWeight = weightMethod match {
      case Some("PostDistVar") => {
        /*
        Use the actual cumulative positive sample distribution to weight large ConfigValue (# pos > Threshold)
        e.g. If # pos in first 2 days took 40% of total, use 0.4 to weight negatives happened 2 days ago
        For small ConfigValue, the weighting is calculated by an exponential function: 1 - Offset * exp( Coef * DayDiff )
         */
        // broadcast join here cause adGroupRangeConvDist is small
        negativeSamples
        .withColumn("BidDiffDayInSeconds", (unix_timestamp(date_add(lit(date), 1)) - unix_timestamp($"LogEntryTimeStr","yyyy-MM-dd HH:mm:ss")) / 86400)
        .withColumn("BidDiffDayInt", floor($"BidDiffDayInSeconds"))
        .join(broadcast(adGroupRangeConvDist), Seq("CampaignIdEncoded", "BidDiffDayInt"), "left")
        .withColumn("WeightDecayFunc", lit(1) - lit(methodDistParams.Offset) * exp(lit(methodDistParams.Coefficient) * $"BidDiffDayInSeconds"))
        .withColumn("Weight", when($"CampaignGroupSize" > methodDistParams.Threshold, $"PosPctInNDay").otherwise($"WeightDecayFunc").cast(DoubleType))
      }
      case _ => { // default is non-weighting
        negativeSamples
      }
    }
    negativeWithAdjustedWeight.selectAs[T]
  }

  def generateWeightForPositive(
                                 positiveData: Dataset[PositiveWithRawWeightsRecord],
                                 weightMethod: Option[String] = None,
                                 ctr: Option[Double] = None
                               )(implicit prometheus:PrometheusClient): Dataset[TrainSetRecord] = {
    weightMethod match {
      // todo: placeholder for time decaying weight
//      case Some("TimeDecay") => {
//        positiveData
//          .withColumn("BidTimeToConvert", unix_timestamp($"ConversionTime") - unix_timestamp($"logEntryTime"))
//
//      }
      case _ => { // default is linear
        val CTR = ctr.getOrElse(1) // todo: should be a much smaller value. probably read from report data to get adgroup's ctr.
        positiveData.withColumn("Weight",
          when(($"NormalizedCustomCPAClickWeight".isNull)||($"NormalizedCustomCPAViewthroughWeight".isNull), $"NormalizedPixelWeight") // has pixel weight, then pixel weight
            .when($"IsClickWindowGreater"&&(!$"IsInViewAttributionWindow"), $"NormalizedCustomCPAClickWeight" *CTR) // click window is longer, bid not in view window
            .when($"IsClickWindowGreater"&&$"IsInViewAttributionWindow", $"NormalizedCustomCPAClickWeight" * CTR +$"NormalizedCustomCPAViewthroughWeight" ) // click window is longer, bid in view window
            .when((!$"IsClickWindowGreater")&&$"IsInClickAttributionWindow", $"NormalizedCustomCPAClickWeight" * CTR+$"NormalizedCustomCPAViewthroughWeight" ) // view window is longer,  bid in click window
            .otherwise($"NormalizedCustomCPAViewthroughWeight") // view window is longer, bid not in click window
        ).selectAs[TrainSetRecord](nullIfAbsent = true)
      }
    }
  }

  /**
   * @param negativeData  negative dataset
   * @param positiveData  positive dataset
   * @param lookBackDays  max lookback days to generate the time frame for distribution
   * @param weightMethod  indicate the weighting method
   * @param methodDistParams  a map of params for the weighting method
   * @return
   */
  def generateWeightForNegative(
                                 negativeData: Dataset[TrainSetRecord],
                                 positiveData: Dataset[PositiveWithRawWeightsRecord],
                                 lookBackDays: Int,
                                 weightMethod: Option[String] = None,
                                 methodDistParams: NegativeWeightDistParams,
                               )(implicit prometheus: PrometheusClient): Dataset[TrainSetRecord] = {

    val negativeDataWithDateDiff = negativeData
      .withColumn("BidDiffDayInSeconds", (unix_timestamp(date_add(lit(date), 1)) - unix_timestamp($"LogEntryTime")) / 86400)
      .withColumn("BidDiffDayInt", floor($"BidDiffDayInSeconds"))

    val adGroupDailyConvDist = positiveData
      .withColumn("BidDiffDayInt", floor((unix_timestamp($"ConversionTime") - unix_timestamp($"LogEntryTime")) / 86400).cast("int"))
      .groupBy("ConfigKey", "ConfigValue", "BidDiffDayInt").agg(count($"BidRequestId").alias("PosDailyCnt"))

    // make sure the distribution is ranged from 0 ~ lookback without missing dates
    val dayRange = (0 to lookBackDays).toList
    val adGroupRangeDF = adGroupDailyConvDist.select("ConfigKey", "ConfigValue").distinct()
      .withColumn("DateRange", typedLit(dayRange))
      .withColumn("BidDiffDayInt", explode($"DateRange"))
      .drop("DateRange")

    val adGroupRangeConvDist = adGroupRangeDF
      .join(adGroupDailyConvDist, Seq("ConfigKey", "ConfigValue", "BidDiffDayInt"), "left")
      .withColumn("PosDailyCnt", coalesce('PosDailyCnt, lit(0)))
      .withColumn("AdGroupSize", sum("PosDailyCnt").over(Window.partitionBy("ConfigKey", "ConfigValue")))
      .withColumn("PosPctInNDay", sum("PosDailyCnt").over(Window.partitionBy("ConfigKey", "ConfigValue").orderBy("BidDiffDayInt"))
        / $"AdGroupSize")

    weightMethod match {
      case Some("PostDistVar") => {
        /*
        Use the actual cumulative positive sample distribution to weight large ConfigValue (# pos > Threshold)
        e.g. If # pos in first 2 days took 40% of total, use 0.4 to weight negatives happened 2 days ago
        For small ConfigValue, the weighting is calculated by an exponential function: 1 - Offset * exp( Coef * DayDiff )
         */
        // broadcast join here cause adGroupRangeConvDist is small
        negativeDataWithDateDiff.join(broadcast(adGroupRangeConvDist), Seq("ConfigKey", "ConfigValue", "BidDiffDayInt"), "left")
          .withColumn("WeightDecayFunc", lit(1) - lit(methodDistParams.Offset) * exp(lit(methodDistParams.Coefficient) * $"BidDiffDayInSeconds"))
          .withColumn("Weight", when($"AdGroupSize" > methodDistParams.Threshold, $"PosPctInNDay").otherwise($"WeightDecayFunc").cast(DoubleType))
          .selectAs[TrainSetRecord](nullIfAbsent = true)
      }
      case _ => { // default is non-weighting
        negativeData
      }
    }
  }

  // will use this function and retire the one above once we switch on daily trainset gen.
  def attachTrainsetWithFeature(
                                      trainset: Dataset[PreFeatureJoinRecord],
                                      mapping: Dataset[AdGroupPolicyMappingRecord],
                                      date: LocalDate,
                                      lookbackDays: Int
                                      )(implicit prometheus: PrometheusClient): Dataset[TrainSetFeaturesRecord] = {
    val bidimpression = if (lookbackDays == 0) {
      DailyBidsImpressionsDataset().readDate(date)
    } else {
      DailyBidsImpressionsDataset().readRange(date.minusDays(lookbackDays), date, isInclusive = true)
    }

    val encodedAdGroupId = mapping.select('AdGroupId, 'AdGroupIdEncoded).distinct()
    val encodedCampaignId = mapping.select('CampaignId, 'CampaignIdEncoded).distinct()
    val encodedAdvertiserId = mapping.select('AdvertiserId, 'AdvertiserIdEncoded).distinct()

    // attachTrainsetWithFeature function
    val df = trainset.join(bidimpression.drop("AdGroupId", "LogEntryTime"), Seq("BidRequestId"), joinType = "inner")
      .withColumn("LogEntryTime", date_format($"LogEntryTime", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("AdFormat", concat(col("AdWidthInPixels"), lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("HasUserData", when($"MatchedSegments".isNull||size($"MatchedSegments")===lit(0), lit(0)).otherwise(lit(1)))
      .withColumn("UserDataLength", when($"UserSegmentCount".isNull, lit(0.0)).otherwise($"UserSegmentCount"*lit(1.0)))
      .withColumn("UserData", when($"HasUserData"===lit(0), lit(null)).otherwise($"MatchedSegments"))
      .withColumn("UserDataOptIn", lit(1))
      .withColumnRenamed("ConfigValue", "AdGroupId")
      .join(broadcast(encodedAdGroupId), Seq("AdGroupId"), "inner")
      .join(broadcast(encodedCampaignId), Seq("CampaignId"), "inner")
      .join(broadcast(encodedAdvertiserId), Seq("AdvertiserId"), "inner")

    val dimAudienceId = AudienceIdTransform.generateAudiencelist(df.select("AdvertiserId", "CampaignId").distinct.selectAs[AudienceFeature], date)
    val dimIndustryCategoryId = AdvertiserFeatureDataSet().readLatestPartitionUpTo(date, isInclusive = true)
      .select("AdvertiserId", "IndustryCategoryId")
      .withColumn("IndustryCategoryId", col("IndustryCategoryId").cast("Int"))

    generateContextualFeatureTier1(df)
      .join(broadcast(dimAudienceId), Seq("AdvertiserId", "CampaignId"), "left")
      .join(broadcast(dimIndustryCategoryId), Seq("AdvertiserId"), "left")
      .selectAs[TrainSetFeaturesRecord]
  }

  def balancePosNeg(
                     realPositives: Dataset[TrainSetRecordVerbose],
                     realNegatives: Dataset[TrainSetRecord],
                     desiredNegOverPos:Int = 9,
                     maxPositiveCount: Int = 500000,
                     maxNegativeCount: Int = 500000,
                     balanceMethod: Option[String] = None,
                     sampleValSet: Boolean = true,
                     samplingSeed: Long
                   )(implicit prometheus:PrometheusClient): Tuple2[Dataset[TrainSetRecordVerbose], Dataset[TrainSetRecord]] = {
/*
1. upsampling vs. class weight https://datascience.stackexchange.com/questions/44755/why-doesnt-class-weight-resolve-the-imbalanced-classification-problem/44760#44760
2. smote: https://github.com/mjuez/approx-smote
 */
    balanceMethod match {
      case Some("upsampling") => upSamplingBySamplyByKey(realPositives, realNegatives, desiredNegOverPos, maxNegativeCount, sampleValSet, samplingSeed)
//        case Some("smote") =>
      case Some("downsampling") => downsampleByKeyByDate(realPositives, realNegatives, desiredNegOverPos, maxPositiveCount, sampleValSet, samplingSeed)
      case _ => downsampleByKeyByDate(realPositives, realNegatives, desiredNegOverPos, maxPositiveCount, sampleValSet, samplingSeed)
    }

  }

  /**
   *
   * @param realPositives positive dataset
   * @param realNegatives negative dataset
   * @param desiredNegOverPos the ratio of neg over pos per adgroup to feed in model training
   * @param maxNegativeCount an upperbound of negative samples per adgroup. will probably remove this is in the future daily negative sampling gives smaller output.
   * @return
   */
  def upSamplingBySamplyByKey(
                               realPositives: Dataset[TrainSetRecordVerbose],
                               realNegatives: Dataset[TrainSetRecord],
                               desiredNegOverPos:Int = 9,
                               maxNegativeCount: Int = 500000,
                               upSamplingValSet: Boolean = false,
                               samplingSeed: Long
                             ): Tuple2[Dataset[TrainSetRecordVerbose], Dataset[TrainSetRecord]] ={

    // 1. randomly throw out negatives if it's more than maxNegativeCount
    /*
    Validation negatives would be affected here.
    The result is the negatives will be less in val set, while the positive of val stays the same.
     During model training, the AUC of val set reflects the ranking of positive compared to negative.
      Since we are randomly throwing out negatives, the ranking should not change because the distribution of pos and neg doesn't change,
      especially when there are many negatives (~500000). We don't need to worry too much.
     */
    val negativeCountsRaw = realNegatives.groupBy("ConfigValue", "ConfigKey").agg(count($"BidRequestID").as("NegBidCount"))
      .withColumn("RetainRate", least(lit(maxNegativeCount)/$"NegBidCount", lit(1)))

    val downSampledNegatives = realNegatives.join(negativeCountsRaw, Seq("ConfigValue", "ConfigKey") )
      .withColumn("Rand", rand(seed=samplingSeed) )
      .filter($"Rand"<=$"RetainRate")
      .selectAs[TrainSetRecord]
      .cache()

    // 2. only resample train records we don't want to up sample val records
    val (positivesToResample, negativeToResample) = upSamplingValSet match {
      case false => {
        (realPositives.filter($"IsInTrainSet"===lit(true)).cache(), downSampledNegatives.filter($"IsInTrainSet"===lit(true)).cache() )
      }
      case _ => (realPositives, downSampledNegatives)
    }

    // 3. calculate groupwise bid count for pos and neg, then calculate upsampling rate
    val positiveCounts = positivesToResample.groupBy("ConfigValue", "ConfigKey").agg(count($"BidRequestId").as("PosBidCount"))
    val negativeCounts = negativeToResample.groupBy("ConfigValue", "ConfigKey").agg(count($"BidRequestID").as("NegBidCount"))

    val upSamplingFraction = negativeCounts.join(positiveCounts, Seq("ConfigValue", "ConfigKey"), "inner")
      .withColumn("Criteria", $"NegBidCount"/(lit(desiredNegOverPos)*$"PosBidCount"))
      .withColumn("UpSamplingPosFraction", when($"Criteria">1, $"Criteria").otherwise(null))
      .withColumn("UpSamplingNegFraction", when($"Criteria"<1, lit(1)/$"Criteria").otherwise(null))
      .cache()

    // get upsampling rate for pos
    val posUpSamplingFraction = upSamplingFraction
      .filter($"UpSamplingPosFraction".isNotNull)
      .selectAs[UpSamplingPosFractionRecord].rdd.map(x =>((x.ConfigValue, x.ConfigKey) , x.UpSamplingPosFraction)).collectAsMap()

    // get upsampling rate for neg
    val negUpSamplingFraction = upSamplingFraction
      .filter($"UpSamplingNegFraction".isNotNull)
      .selectAs[UpSamplingNegFractionRecord].rdd.map(x =>((x.ConfigValue, x.ConfigKey) , x.UpSamplingNegFraction)).collectAsMap()

    // 4. upsampling pos  and neg
    val upSampledPos = positivesToResample
      .join(upSamplingFraction.filter($"UpSamplingPosFraction".isNotNull), Seq("ConfigValue", "ConfigKey"),"leftsemi").selectAs[TrainSetRecord]
      .rdd.keyBy(x => ( x.ConfigValue, x.ConfigKey)).sampleByKey(true, posUpSamplingFraction, seed=samplingSeed)
      .map(x=> x._2)
      .toDF()
      .selectAs[TrainSetRecordVerbose]

    val upSampledNeg = negativeToResample
      .join(upSamplingFraction.filter($"UpSamplingNegFraction".isNotNull), Seq("ConfigValue", "ConfigKey"),"leftsemi").selectAs[TrainSetRecord]
      .rdd.keyBy(x => ( x.ConfigValue, x.ConfigKey)).sampleByKey(true, negUpSamplingFraction, seed=samplingSeed)
      .map(x=> x._2)
      .toDF()
      .selectAs[TrainSetRecord]

    // 5. get non sampled pos and neg
    val nonSampledPos = positivesToResample.join( upSamplingFraction.filter($"UpSamplingPosFraction".isNull), Seq("ConfigValue", "ConfigKey"), "leftsemi")
      .selectAs[TrainSetRecordVerbose]

    val nonSampledNeg = negativeToResample.join( upSamplingFraction.filter($"UpSamplingNegFraction".isNull), Seq( "ConfigValue", "ConfigKey"), "leftsemi")
      .selectAs[TrainSetRecord]

    upSamplingValSet match {
      case false => {
        (
          //remove config values that are abandoned due to absence of pos or neg by joining with upSamplingFraction
          upSampledPos
            .union(nonSampledPos)
            .union(realPositives.filter($"IsInTrainSet"===lit(false)).join( upSamplingFraction, Seq("ConfigValue", "ConfigKey"), "leftsemi").selectAs[TrainSetRecordVerbose] ),
          upSampledNeg
            .union(nonSampledNeg)
            .union(downSampledNegatives.filter($"IsInTrainSet"===lit(false)).join( upSamplingFraction, Seq("ConfigValue", "ConfigKey"), "leftsemi").selectAs[TrainSetRecord])
          )
      }
      case _ =>   (upSampledPos.union(nonSampledPos), upSampledNeg.union(nonSampledNeg))
    }

  }

  def downsampleByKeyByDate(
                             realPositives: Dataset[TrainSetRecordVerbose],
                             realNegatives: Dataset[TrainSetRecord],
                             desiredNegOverPos: Int = 9,
                             maxPositiveCount: Int = 500000,
                             sampleValSet: Boolean = true,
                             samplingSeed: Long
                             ): Tuple2[Dataset[TrainSetRecordVerbose], Dataset[TrainSetRecord]] = {

    val (positivesToResample, negativeToResample) = sampleValSet match {
      case false => {
        (realPositives.filter($"IsInTrainSet" === lit(true)).cache(), realNegatives.filter($"IsInTrainSet" === lit(true)).cache())
      }
      case _ => (realPositives, realNegatives)
    }

    val downSampledPositives = positivesToResample
      .withColumn("PositiveCount", count("BidRequestId").over(Window.partitionBy("ConfigKey", "ConfigValue")))
      .withColumn("Ratio", lit(maxPositiveCount) / $"PositiveCount")
      .withColumn("Rand", rand(seed = samplingSeed))
      .filter($"Rand" <= $"Ratio")
      .drop("Rand", "Ratio", "PositiveCount")

    val positiveDailyCount = downSampledPositives
      .withColumn("LogEntryDate", to_date($"LogEntryTime"))
      .groupBy("ConfigKey", "ConfigValue", "LogEntryDate")
      .count()
      .withColumnRenamed("count", "PosDailyCount")

    val downSampledNegatives = negativeToResample
      .withColumn("LogEntryDate", to_date($"LogEntryTime"))
      .withColumn("NegDailyCount",
        count("BidRequestId").over(Window.partitionBy("ConfigKey", "ConfigValue", "LogEntryDate")))
      .join(broadcast(positiveDailyCount), Seq("ConfigKey", "ConfigValue", "LogEntryDate"), "left")
      .withColumn("Ratio", lit(desiredNegOverPos) * $"PosDailyCount" / $"NegDailyCount")
      .withColumn("Rand", rand(seed = samplingSeed))
      .filter($"Rand" <= $"Ratio")

    sampleValSet match {
      case false => {
        (
          // positives is down sampled also
          downSampledPositives.selectAs[TrainSetRecordVerbose].union(realPositives.filter($"IsInTrainSet" === lit(false))),
          downSampledNegatives.selectAs[TrainSetRecord].union(realNegatives.filter($"IsInTrainSet" === lit(false)))
        )
      }
      case _ => (downSampledPositives.selectAs[TrainSetRecordVerbose], downSampledNegatives.selectAs[TrainSetRecord])
    }
  }

  def adjustWeightForTrainset(trainset: Dataset[PreFeatureJoinRecord],desiredNegOverPos: Int ):
  Dataset[PreFeatureJoinRecord] = {
    val train = trainset.filter($"IsInTrainSet" === lit(true))
    val validation = trainset.filter($"IsInTrainSet" === lit(false))
    val sumWeight = train.filter(col("Target")===0).groupBy("ConfigKey", "ConfigValue").agg(sum("Weight").as("NegSumWeight"))
      .join(train.filter(col("Target")===1).groupBy("ConfigKey", "ConfigValue").agg(sum("Weight").as("PosSumWeight")), Seq("ConfigKey", "ConfigValue"),"inner")
    // We observed that there are some adgroups where its pos-weight is 0, which is caused by user settings.May solve it in the future.
    val adjustedTrainset = sumWeight.withColumn("Coefficient",when($"PosSumWeight">0,$"NegSumweight"/$"PosSumWeight").otherwise($"NegSumWeight"))
      .join(train, Seq("ConfigKey", "ConfigValue"), "inner")
      .withColumn("Weight",when(col("Target")===1,col("Weight")*col("Coefficient")/lit(desiredNegOverPos)).otherwise(col("Weight")))
      .selectAs[PreFeatureJoinRecord].toDF()
    val orgValidationset = validation.toDF()
    adjustedTrainset.union(orgValidationset).selectAs[PreFeatureJoinRecord]
  }

  def balanceWeightForTrainset(trainset: Dataset[UserDataValidationDataForModelTrainingRecord], desiredNegOverPos: Int): Dataset[UserDataValidationDataForModelTrainingRecord] = {
    // Cache the trainset as we are using it multiple times
    val cachedTrainset = trainset.cache()

    val sumWeight = cachedTrainset.filter(col("Target") === 0).groupBy("AdGroupIdEncoded").agg(sum("Weight").as("NegSumWeight"))
    .join(cachedTrainset.filter(col("Target") === 1).groupBy("AdGroupIdEncoded").agg(sum("Weight").as("PosSumWeight")), Seq("AdGroupIdEncoded"), "inner")

    // Repartition to distribute data evenly across partitions
    val repartitionedSumWeight = 
      sumWeight
      .withColumn("Coefficient", when($"PosSumWeight" > 0, $"NegSumweight" / $"PosSumWeight").otherwise($"NegSumWeight"))
      .repartition(col("AdGroupIdEncoded"))

    val adjustedTrainset = cachedTrainset.join(broadcast(repartitionedSumWeight), Seq("AdGroupIdEncoded"), "inner")
          .withColumn("Weight", when(col("Target") === 1, col("Weight") * col("Coefficient") / lit(desiredNegOverPos)).otherwise(col("Weight")).cast("float"))

    adjustedTrainset.selectAs[UserDataValidationDataForModelTrainingRecord]
  }
}
