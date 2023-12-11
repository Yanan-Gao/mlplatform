package com.thetradedesk.kongming.transform

import com.thetradedesk.kongming
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, BidsImpressionsSchema, BidRequestPolicyRecord, DailyBidRequestRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.kongming.{multiLevelJoinWithPolicy, RoundUpTimeUnit}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{date_trunc, dense_rank, max}

import java.time.LocalDate

object BidRequestTransform {
  val DEFAULT_NUM_PARTITION = 200

  def dailyTransform(bidsImpressions: Dataset[BidsImpressionsSchema],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[DailyBidRequestRecord] = {
    val bidsImpressionFilterByPolicy = multiLevelJoinWithPolicy[BidRequestPolicyRecord](
      bidsImpressions.filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")
,
      adGroupPolicy.select("DataAggKey", "DataAggValue", "LastTouchCount").dropDuplicates("DataAggKey", "DataAggValue").cache(),
      "inner")

    bidsImpressionFilterByPolicy
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .withColumn("RecencyRank", dense_rank().over(Window.partitionBy($"DataAggKey", $"DataAggValue", $"UIID").orderBy($"TruncatedLogEntryTime".desc)))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .selectAs[DailyBidRequestRecord]
  }
}

