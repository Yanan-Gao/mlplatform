package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.kongming.RoundUpTimeUnit
import com.thetradedesk.kongming.datasets.DailyConversionDataRecord
import com.thetradedesk.kongming.datasets.DailyPositiveLabelRecord
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyBidRequestRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.date_trunc
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.greatest
import org.apache.spark.sql.functions.least
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{broadcast, row_number}

import java.time.LocalDateTime
import java.sql.Timestamp

object PositiveLabelDailyTransform {

case class DailyPositiveBidRequestRecord(
                                          ConfigKey: String,
                                          ConfigValue: String,
                                          DataAggKey: String,
                                          DataAggValue: String,
                                          BidRequestId: String,
                                          TrackingTagId: String,
                                          UIID: String,
                                          ConversionTime: java.sql.Timestamp,
                                          LogEntryTime: java.sql.Timestamp,
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
                                         dailyConversionDS : Dataset[DailyConversionDataRecord]
                                       )
                    (implicit prometheus: PrometheusClient): Dataset[DailyPositiveBidRequestRecord] = {

    val filteredBidRequest= bidsImpressions
      //TODO: assumed aggValue is on adgroup level, will need to be more comprehensive later on.
      .join(broadcast(adGroupPolicy), bidsImpressions("AdGroupId") === adGroupPolicy("DataAggValue"))
      .filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")

    val window = Window.partitionBy($"DataAggKey", $"DataAggValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"TruncatedLogEntryTime".desc)
      filteredBidRequest.as("t1")
      .join(dailyConversionDS.as("t2"),
        filteredBidRequest("UIID")===dailyConversionDS("UIID") &&
        filteredBidRequest("DataAggKey")===dailyConversionDS("DataAggKey") &&
        filteredBidRequest("DataAggValue")===dailyConversionDS("DataAggValue") &&
        filteredBidRequest("LogEntryTime")<=dailyConversionDS("ConversionTime"),
        "inner"
      )
      .select("t1.BidRequestId",
      "t1.ConfigKey",
        "t1.ConfigValue",
        "t1.DataAggKey",
        "t1.DataAggValue",
        "t1.LogEntryTime",
        "t1.IsImp",
        "t1.BidRequestId",
        "t2.TrackingTagId",
        "t2.UIID",
        "t2.ConversionTime"
      )
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

    // TODO: code will break if data agg key is not adgroupid. will need to revisit this later.
    val window = Window.partitionBy($"DataAggKey", $"DataAggValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"LogEntryTime".desc)

    multidayBidImpressionDS
      .join(broadcast(adGroupPolicy), Seq("ConfigKey","ConfigValue","DataAggKey","DataAggValue"))
      .join(dailyConversionDS,
        Seq("UIID","DataAggValue", "DataAggKey"),
        "inner"
      )
//      .withColumn("DataLookBackInSeconds", $"DataLookBack"*24*3600)
//      .withColumn("lookbackInSeconds",
//        least(
//          greatest($"AttributionClickLookbackWindowInSeconds",$"AttributionImpressionLookbackWindowInSeconds")
//          , $"DataLookBackInSeconds")
//      )
//      .filter($"lookbackInSeconds">=unix_timestamp(lit(Timestamp.valueOf(endDateTime)) )- unix_timestamp($"logEntryTime"))
//      .withColumn("RecencyRank", row_number().over(window))
//      .filter($"RecencyRank" <= $"LastTouchCount")
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

    val window = Window.partitionBy($"DataAggKey", $"DataAggValue", $"UIID", $"TrackingTagId", $"ConversionTime").orderBy($"TruncatedLogEntryTime".desc)

    unionedMultidayPositive
    .join(broadcast(adGroupPolicy), Seq("ConfigKey","ConfigValue","DataAggKey","DataAggValue"))
    .withColumn("DataLookBackInSeconds", $"DataLookBack"*24*3600)
    .withColumn("lookbackInSeconds",
      least(
        greatest($"AttributionClickLookbackWindowInSeconds",$"AttributionImpressionLookbackWindowInSeconds")
        , $"DataLookBackInSeconds")
    )
    .withColumn("TouchConvTimeDiffInSeconds", unix_timestamp($"ConversionTime") - unix_timestamp($"LogEntryTime"))
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

}
