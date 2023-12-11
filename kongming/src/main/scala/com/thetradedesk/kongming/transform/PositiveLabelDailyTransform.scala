package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming.RoundUpTimeUnit
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.multiLevelJoinWithPolicy
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate

object PositiveLabelDailyTransform {
  case class DailyPositiveBidRequestRecord(
                                          ConfigKey: String,
                                          ConfigValue: String,
                                          BidRequestId: String,
                                          TrackingTagId: String,
                                          UIID: String,
                                          ConversionTime: java.sql.Timestamp,
                                          LogEntryTime: java.sql.Timestamp,
                                          MonetaryValue: Option[BigDecimal],
                                          MonetaryValueCurrency: Option[String],
                                          IsImp: Boolean
                                        )
  /**
   * function dealing with raw bidrequest and conversion joining for the same day as conversion data.
   * @param bidsImpressions
   * @param adGroupPolicy
   * @param dailyConversionDS
   * @param prometheus
   * @return ranked bidrequest data filtered by conversions. Last N touches only.
   */
  def intraDayConverterNTouchesTransform(
                                         bidsImpressions: Dataset[BidsImpressionsSchema],
                                         adGroupPolicy: Dataset[AdGroupPolicyRecord],
                                         dailyConversionDS : Dataset[DailyConversionDataRecord],
                                         adGroupDS: Dataset[AdGroupRecord]
                                       )
                    (implicit prometheus: PrometheusClient): Dataset[DailyPositiveBidRequestRecord] = {

    // tag bidImpressions with policy
    val filteredBidRequest = bidsImpressions.filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")

    val sameDayCampaignPositiveBidRequests = dailyConversionDS
      .join(broadcast(adGroupPolicy.filter('DataAggKey === lit("CampaignId")).select("ConfigKey", "ConfigValue", "DataAggValue", "LastTouchCount")), Seq("ConfigKey", "ConfigValue"), "inner")
      .join(filteredBidRequest.withColumnRenamed("CampaignId", "DataAggValue"), Seq("UIID", "DataAggValue"), "inner")
    val sameDayAdvertiserPositiveBidRequests = dailyConversionDS
      .join(broadcast(adGroupPolicy.filter('DataAggKey === lit("AdvertiserId")).select("ConfigKey", "ConfigValue", "DataAggValue", "LastTouchCount")), Seq("ConfigKey", "ConfigValue"), "inner")
      .join(filteredBidRequest.withColumnRenamed("AdvertiserId", "DataAggValue"), Seq("UIID", "DataAggValue"), "inner")

    val window = Window.partitionBy($"ConfigKey", $"ConfigValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"TruncatedLogEntryTime".desc)

    sameDayCampaignPositiveBidRequests.union(sameDayAdvertiserPositiveBidRequests)
      .filter($"LogEntryTime"<=$"ConversionTime")
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .withColumn("RecencyRank", dense_rank().over(window))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .selectAs[DailyPositiveBidRequestRecord]
  }

  /**
   * filter multiday bids data down to conversion pool.
   * @param multidayBidImpressionDS multiday processed last n touches.
   * @param dailyConversionDS single day conversion.
   * @param adGroupPolicy policy table
   * @param endDateTime
   * @param prometheus
   * @return previous dates bids data associated with converters. Last N touches only.
   */
  def multiDayConverterTransform(
                                  multidayBidImpressionDS: Dataset[DailyBidRequestRecord],
                                  dailyConversionDS : Dataset[DailyConversionDataRecord],
                                  adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                )
                                (implicit prometheus: PrometheusClient): Dataset[DailyPositiveBidRequestRecord] = {
    dailyConversionDS
      .join(broadcast(adGroupPolicy.select("ConfigKey", "ConfigValue", "DataAggKey", "DataAggValue")), Seq("ConfigKey", "ConfigValue"), "inner")
      .join(multidayBidImpressionDS, Seq("UIID", "DataAggKey", "DataAggValue"), "inner")
      .selectAs[DailyPositiveBidRequestRecord]
  }
  //TODO: below function is commented out but might be worth testing for calculation efficiency.
//  /**
//   * filter multiday bids data down to conversion pool.
//   * @param multidayBidImpressionDS multiday processed last n touches.
//   * @param dailyConversionDS single day conversion.
//   * @param adGroupPolicy policy table
//   * @param endDateTime
//   * @param prometheus
//   * @return previous dates bids data associated with converters. Last N touches only.
//   */
//  def multiDayConverterTransform(
//                                 multidayBidImpressionDS: Dataset[DailyBidRequestRecord],
//                                 dailyConversionDS : Dataset[DailyConversionDataRecord],
//                                 adGroupPolicy: Dataset[AdGroupPolicyRecord],
//                                 endDateTime: LocalDateTime
//                               )
//                                       (implicit prometheus: PrometheusClient): Dataset[DailyPositiveBidRequestRecord] = {
//
//    // TODO: code will break if data agg key is not adgroupid. will need to revisit this later.
//    val window = Window.partitionBy($"DataAggKey", $"DataAggValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"LogEntryTime".desc)
//
//    multidayBidImpressionDS
//      .join(broadcast(adGroupPolicy), Seq("ConfigKey","ConfigValue","DataAggKey","DataAggValue"))
//      .join(dailyConversionDS,
//        Seq("UIID","DataAggValue", "DataAggKey"),
//        "inner"
//      )
//      .withColumn("DataLookBackInSeconds", $"DataLookBack"*24*3600)
//      .withColumn("lookbackInSeconds",
//        least(
//          greatest($"AttributionClickLookbackWindowInSeconds",$"AttributionImpressionLookbackWindowInSeconds")
//          , $"DataLookBackInSeconds")
//      )
//      .filter($"lookbackInSeconds">=unix_timestamp(lit(Timestamp.valueOf(endDateTime)) )- unix_timestamp($"logEntryTime"))
//      .withColumn("RecencyRank", row_number().over(window))
//      .filter($"RecencyRank" <= $"LastTouchCount")
//      .selectAs[DailyPositiveBidRequestRecord]
//  }

  /**
   * Aggregate data across all bids prior to the conversion.
   * @param unionedMultidayPositive
   * @param adGroupPolicy
   * @return Last N touches.
   */
  def positiveLabelAggTransform(
                               unionedMultidayPositive:Dataset[DailyPositiveBidRequestRecord]
                               , adGroupPolicy: Dataset[AdGroupPolicyRecord]
                               ):Dataset[DailyPositiveLabelRecord]={

    val window = Window.partitionBy($"ConfigKey", $"ConfigValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"TruncatedLogEntryTime".desc)

    unionedMultidayPositive
      .join(broadcast(adGroupPolicy), Seq("ConfigKey","ConfigValue"), "inner")
      .withColumn("DataLookBackInSeconds", $"DataLookBack"*24*3600)
      .withColumn("lookbackInSeconds",
        least(
          greatest($"AttributionClickLookbackWindowInSeconds", $"AttributionImpressionLookbackWindowInSeconds")
          , $"DataLookBackInSeconds")
        )
      .withColumn("TouchConvTimeDiffInSeconds", unix_timestamp($"ConversionTime") - unix_timestamp($"LogEntryTime"))
      .filter($"TouchConvTimeDiffInSeconds" >= 0)
      .filter($"lookbackInSeconds">=$"TouchConvTimeDiffInSeconds")
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .withColumn("RecencyRank", dense_rank().over(window))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .withColumn(
        "IsClickWindowGreater", $"AttributionClickLookbackWindowInSeconds">$"AttributionImpressionLookbackWindowInSeconds"
      )
      .withColumn(
        "IsInClickAttributionWindow",
        when($"TouchConvTimeDiffInSeconds"<=$"AttributionClickLookbackWindowInSeconds", true).otherwise(false)
      )
      .withColumn(
        "IsInViewAttributionWindow",
        when($"TouchConvTimeDiffInSeconds"<=$"AttributionImpressionLookbackWindowInSeconds", true).otherwise(false)
      )
      .selectAs[DailyPositiveLabelRecord]
  }

  def countDataAggGroupPositives(
                                  positives: Dataset[DailyPositiveLabelRecord],
                                  adGroups: Dataset[AdGroupRecord]
                                ): Dataset[DailyPositiveCountSummaryRecord] = {
    positives.join(adGroups.select("AdGroupId", "CampaignId", "AdvertiserId"), 'AdGroupId === 'ConfigValue, "inner")
      .groupBy("AdGroupId", "CampaignId", "AdvertiserId").count()
      .selectAs[DailyPositiveCountSummaryRecord]
  }
}
