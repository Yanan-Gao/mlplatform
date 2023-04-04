package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.shiftModUdf
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.ContextualTransform.ContextualData
import com.thetradedesk.kongming.{date, multiLevelJoinWithPolicy}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import job.GenerateTrainSet.modelDimensions
import org.apache.spark.sql.types.DoubleType

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
                                   //                                     AdGroupId: String,
                                   ConfigKey: String,
                                   ConfigValue: String,
                                   //                                       DataAggKey: String,
                                   //                                       DataAggValue: String,
                                   BidRequestId: String,
                                   Weight: Double,
                                   LogEntryTime:  java.sql.Timestamp,
                                   Target: Int,
                                   IsInTrainSet: Boolean

                                 )

  case class TrainSetRecord(
                             ConfigKey: String,
                             ConfigValue: String,
                             DataAggKey: String,
                             DataAggValue: String,
                             BidRequestId: String,
                             Weight: Double,
                             LogEntryTime:  java.sql.Timestamp,
                             IsInTrainSet: Boolean
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
                                           DataAggKey: String,
                                           DataAggValue: String,

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


  case class BaseAssociateAdGroupMapping(
                                        AdGroupId: String,
                                        ConfigValue: String,
                                      )

  def getWeightsForTrackingTags(
                                 date: LocalDate,
                                 adGroupPolicy: Dataset[AdGroupPolicyRecord],
                                 adGroupDS: Dataset[AdGroupRecord],
                                 normalized: Boolean=false
                               ): Dataset[TrackingTagWeightsRecord]= {
    // 1. get the latest weights per campaign and trackingtagid
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date)
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date)
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
      .select($"CampaignId",$"TrackingTagId", $"ReportingColumnId", $"NormalizedPixelWeight".cast(DoubleType),$"NormalizedCustomCPAClickWeight".cast(DoubleType), $"NormalizedCustomCPAViewthroughWeight".cast(DoubleType) )

    // 2. join trackingtag weights with policy table
    adGroupPolicy
      .join(broadcast(adGroupDS), adGroupPolicy("ConfigValue")===adGroupDS("AdGroupId"), "inner")
      .select("CampaignId","ConfigKey", "ConfigValue", "DataAggKey", "DataAggValue")
      .join(ccrcProcessed, Seq("CampaignId"), "inner")
      .select($"TrackingTagId", $"ReportingColumnId", $"ConfigKey", $"ConfigValue", $"NormalizedPixelWeight",$"NormalizedCustomCPAClickWeight", $"NormalizedCustomCPAViewthroughWeight" ) // one trackingtagid can have different following values
      .selectAs[TrackingTagWeightsRecord]

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
        ).selectAs[TrainSetRecord]
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
          .selectAs[TrainSetRecord]
      }
      case _ => { // default is non-weighting
        negativeData
      }
    }
  }


  def attachTrainsetWithFeature(
                          trainset: Dataset[PreFeatureJoinRecord],
                          lookbackDays: Int
                          )(implicit prometheus:PrometheusClient): Dataset[TrainSetFeaturesRecord] ={
    val bidsImpressions = DailyBidsImpressionsDataset().readRange(date.minusDays(lookbackDays), date, isInclusive = true)

    val df = trainset.join(bidsImpressions.drop("AdGroupId"), Seq("BidRequestId"), joinType =  "inner")
      .withColumn("AdFormat",concat(col("AdWidthInPixels"),lit('x'), col("AdHeightInPixels")))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .withColumnRenamed("ConfigValue", "AdGroupId")

    val bidsImpContextual = ContextualTransform
      .generateContextualFeatureTier1(
        df.select("BidRequestId", "ContextualCategories")
          .dropDuplicates("BidRequestId").selectAs[ContextualData]
      )

    df
      .join(bidsImpContextual, Seq("BidRequestId"), "left")
      .selectAs[TrainSetFeaturesRecord]
  }

  def balancePosNeg(
                     realPositives: Dataset[TrainSetRecord],
                     realNegatives: Dataset[TrainSetRecord],
                     desiredNegOverPos:Int = 9,
                     maxNegativeCount: Int = 500000,
                     balanceMethod: Option[String] = None,
                     sampleValSet: Boolean = true,
                     samplingSeed: Long
                   )(implicit prometheus:PrometheusClient): Tuple2[Dataset[TrainSetRecord], Dataset[TrainSetRecord]] = {
/*
1. upsampling vs. class weight https://datascience.stackexchange.com/questions/44755/why-doesnt-class-weight-resolve-the-imbalanced-classification-problem/44760#44760
2. smote: https://github.com/mjuez/approx-smote
 */
    balanceMethod match {
      case Some("upsampling") => upSamplingBySamplyByKey(realPositives, realNegatives, desiredNegOverPos, maxNegativeCount, sampleValSet, samplingSeed)
//        case Some("smote") =>
      case Some("downsampling") => downsampleNegByKeyByDate(realPositives, realNegatives, desiredNegOverPos, sampleValSet, samplingSeed)
      case _ => downsampleNegByKeyByDate(realPositives, realNegatives, desiredNegOverPos, sampleValSet, samplingSeed)
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
                                realPositives: Dataset[TrainSetRecord],
                                realNegatives: Dataset[TrainSetRecord],
                                desiredNegOverPos:Int = 9,
                                maxNegativeCount: Int = 500000,
                                upSamplingValSet: Boolean = false,
                                samplingSeed: Long
                             ): Tuple2[Dataset[TrainSetRecord], Dataset[TrainSetRecord]] ={

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
      .selectAs[TrainSetRecord]

    val upSampledNeg = negativeToResample
      .join(upSamplingFraction.filter($"UpSamplingNegFraction".isNotNull), Seq("ConfigValue", "ConfigKey"),"leftsemi").selectAs[TrainSetRecord]
      .rdd.keyBy(x => ( x.ConfigValue, x.ConfigKey)).sampleByKey(true, negUpSamplingFraction, seed=samplingSeed)
      .map(x=> x._2)
      .toDF()
      .selectAs[TrainSetRecord]

    // 5. get non sampled pos and neg
    val nonSampledPos = positivesToResample.join( upSamplingFraction.filter($"UpSamplingPosFraction".isNull), Seq("ConfigValue", "ConfigKey"), "leftsemi")
      .selectAs[TrainSetRecord]

    val nonSampledNeg = negativeToResample.join( upSamplingFraction.filter($"UpSamplingNegFraction".isNull), Seq( "ConfigValue", "ConfigKey"), "leftsemi")
      .selectAs[TrainSetRecord]

    upSamplingValSet match {
      case false => {
        (
          //remove config values that are abandoned due to absence of pos or neg by joining with upSamplingFraction
          upSampledPos
            .union(nonSampledPos)
            .union(realPositives.filter($"IsInTrainSet"===lit(false)).join( upSamplingFraction, Seq("ConfigValue", "ConfigKey"), "leftsemi").selectAs[TrainSetRecord] ),
          upSampledNeg
            .union(nonSampledNeg)
            .union(downSampledNegatives.filter($"IsInTrainSet"===lit(false)).join( upSamplingFraction, Seq("ConfigValue", "ConfigKey"), "leftsemi").selectAs[TrainSetRecord])
          )
      }
      case _ =>   (upSampledPos.union(nonSampledPos), upSampledNeg.union(nonSampledNeg))
    }

  }

  def downsampleNegByKeyByDate(
                               realPositives: Dataset[TrainSetRecord],
                               realNegatives: Dataset[TrainSetRecord],
                               desiredNegOverPos: Int = 9,
                               sampleValSet: Boolean = true,
                               samplingSeed: Long
                             ): Tuple2[Dataset[TrainSetRecord], Dataset[TrainSetRecord]] = {

    val (positivesToResample, negativeToResample) = sampleValSet match {
      case false => {
        (realPositives.filter($"IsInTrainSet" === lit(true)).cache(), realNegatives.filter($"IsInTrainSet" === lit(true)).cache())
      }
      case _ => (realPositives, realNegatives)
    }

    val positiveDailyCount = positivesToResample
      .withColumn("LogEntryDate", to_date($"LogEntryTime"))
      .groupBy("DataAggValue", "LogEntryDate")
      .count().withColumnRenamed("count", "PosDailyCount")
    val downSampledNegatives = negativeToResample
      .withColumn("LogEntryDate", to_date($"LogEntryTime"))
      .withColumn("NegDailyCount",
        count("BidRequestId").over(Window.partitionBy("DataAggValue", "LogEntryDate")))
      .join(positiveDailyCount, Seq("DataAggValue", "LogEntryDate"), "left")
      .withColumn("Ratio", lit(desiredNegOverPos) * $"PosDailyCount" / $"NegDailyCount")
      .withColumn("Rand", rand(seed = samplingSeed))
      .filter($"Rand" <= $"Ratio")

    sampleValSet match {
      case false => {
        (
          // positives will not be sampled
          realPositives,
          downSampledNegatives.selectAs[TrainSetRecord].union(realNegatives.filter($"IsInTrainSet" === lit(false)))
        )
      }
      case _ => (realPositives, downSampledNegatives.selectAs[TrainSetRecord])
    }
  }

  def adjustWeightForTrainset(trainset: Dataset[PreFeatureJoinRecord],desiredNegOverPos: Int ):
  Dataset[PreFeatureJoinRecord] = {
    val train = trainset.filter($"IsInTrainSet" === lit(true)).cache()
    val validation = trainset.filter($"IsInTrainSet" === lit(false))
    val sumWeight = train.filter(col("Target")===0).groupBy("ConfigValue").agg(sum("Weight").as("NegSumWeight"))
      .join(train.filter(col("Target")===1).groupBy("ConfigValue").agg(sum("Weight").as("PosSumWeight")),Seq("ConfigValue"),"inner")
    // We observed that there are some adgroups where its pos-weight is 0, which is caused by user settings.May solve it in the future.
    val adjustedTrainset = sumWeight.withColumn("Coefficient",when($"PosSumWeight">0,$"NegSumweight"/$"PosSumWeight").otherwise($"NegSumWeight"))
      .join(train,Seq("ConfigValue"),"inner")
      .withColumn("Weight",when(col("Target")===1,col("Weight")*col("Coefficient")/lit(desiredNegOverPos)).otherwise(col("Weight")))
      .selectAs[PreFeatureJoinRecord].toDF()
    val orgValidationset = validation.toDF()
    adjustedTrainset.union(orgValidationset).selectAs[PreFeatureJoinRecord]
  }

  def getBaseAssociateAdGroupIntMappings(
                                       adGroupPolicy: Dataset[AdGroupPolicyRecord],
                                       adGroupDS: Dataset[AdGroupRecord]): Dataset[BaseAssociateAdGroupMappingIntRecord] = {
    multiLevelJoinWithPolicy[BaseAssociateAdGroupMapping](adGroupDS, adGroupPolicy, "inner")
      .withColumn("AdGroupIdInt", shiftModUdf(xxhash64(col("AdGroupId")), lit(modelDimensions(0).cardinality.getOrElse(0))))
      .withColumnRenamed("ConfigValue", "BaseAdGroupId")
      .withColumn("BaseAdGroupIdInt", shiftModUdf(xxhash64(col("BaseAdGroupId")), lit(modelDimensions(0).cardinality.getOrElse(0))))
      .selectAs[BaseAssociateAdGroupMappingIntRecord]
  }


}



