package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.kongming.RoundUpTimeUnit
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, DailyBidRequestRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.date_trunc
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.{broadcast, row_number}


object BidRequestTransform {
  val DEFAULT_NUM_PARTITION = 200

  def dailyTransform(bidsImpressions: Dataset[BidsImpressionsSchema],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord])
                    (implicit prometheus: PrometheusClient): Dataset[DailyBidRequestRecord] = {
    val window = Window.partitionBy($"AdGroupId", $"UIID").orderBy($"TruncatedLogEntryTime".desc)
    bidsImpressions
      //TODO: need to revisit this later, since it has assumption on the policy grain has only adgroupid.
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .join(broadcast(adGroupPolicy), bidsImpressions("AdGroupId") === adGroupPolicy("DataAggValue"))
      .filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("RecencyRank", dense_rank().over(window))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .selectAs[DailyBidRequestRecord]
  }
}
