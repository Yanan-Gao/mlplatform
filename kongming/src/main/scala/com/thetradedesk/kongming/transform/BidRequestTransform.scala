package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.kongming
import com.thetradedesk.kongming.RoundUpTimeUnit
import com.thetradedesk.kongming.datasets.AdGroupDataset
import com.thetradedesk.kongming.datasets.AdGroupRecord
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, BidRequestPolicyRecord, DailyBidRequestRecord}
import com.thetradedesk.kongming.multiLevelJoinWithPolicy
import com.thetradedesk.kongming.preFilteringWithPolicy
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.date_trunc
import org.apache.spark.sql.functions.dense_rank

object BidRequestTransform {
  val DEFAULT_NUM_PARTITION = 200

  def dailyTransform(bidsImpressions: Dataset[BidsImpressionsSchema],
                     adGroupPolicy: Dataset[AdGroupPolicyRecord]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[DailyBidRequestRecord] = {
    val window = Window.partitionBy($"DataAggValue", $"UIID").orderBy($"TruncatedLogEntryTime".desc)

    val adGroupDS = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, kongming.date)
    val prefilteredDS = preFilteringWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, adGroupDS)
    val bidsImpressionFilterByPolicy = multiLevelJoinWithPolicy[BidRequestPolicyRecord](prefilteredDS, adGroupPolicy)

    bidsImpressionFilterByPolicy
      //TODO: need to revisit this later, since it has assumption on the policy grain has only adgroupid.
      .withColumn("TruncatedLogEntryTime", date_trunc(RoundUpTimeUnit, $"LogEntryTime"))
      .filter($"UIID".isNotNullOrEmpty && $"UIID" =!= "00000000-0000-0000-0000-000000000000")
      .withColumn("RecencyRank", dense_rank().over(window))
      .filter($"RecencyRank" <= $"LastTouchCount")
      .selectAs[DailyBidRequestRecord]
  }
}

