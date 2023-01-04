package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.kongming
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, BidRequestPolicyRecord, DailyBidRequestRecord, UnifiedAdGroupDataSet}
import com.thetradedesk.kongming.{multiLevelJoinWithPolicy, preFilteringWithPolicy, RoundUpTimeUnit}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{date_trunc, dense_rank}

object BidRequestTransform {
  val DEFAULT_NUM_PARTITION = 200

  def dailyTransform(bidsImpressions: Dataset[BidsImpressionsSchema],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[DailyBidRequestRecord] = {
    val window = Window.partitionBy($"DataAggValue", $"UIID").orderBy($"TruncatedLogEntryTime".desc)

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()
    val prefilteredDS = preFilteringWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, adGroupDS)
    val bidsImpressionFilterByPolicy = multiLevelJoinWithPolicy[BidRequestPolicyRecord](prefilteredDS, adGroupPolicy, "inner")

    bidsImpressionFilterByPolicy
      //TODO: need to revisit this later, since it has assumption on the policy grain has only adgroupid.
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("RecencyRank", dense_rank().over(window))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .selectAs[DailyBidRequestRecord]
  }
}

