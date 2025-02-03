package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions

import java.sql.Timestamp
import java.time.LocalDate

object CampaignThrottleMetricDataset extends S3DailyParquetDataset[CampaignThrottleMetricSchema]{
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=${env}/aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_parquet"
  }

  override def readDate(date: LocalDate, env: String, lookBack: Int, nullIfColAbsent: Boolean)
                       (implicit encoder: Encoder[CampaignThrottleMetricSchema], spark: SparkSession): Dataset[CampaignThrottleMetricSchema] = {
    super.readDate(date, env, lookBack, nullIfColAbsent)
      .withColumn("Date", to_date(col("Date")))
      .selectAs[CampaignThrottleMetricSchema]
  }
}

case class CampaignThrottleMetricSchema(
                                          Date: Timestamp,
                                          CampaignId: String,
                                          CampaignFlightId: Long,
                                          IsValuePacing: Boolean,
                                          MinCalculatedCampaignCapInUSD: Double,
                                          MaxCalculatedCampaignCapInUSD: Double,
                                          OverdeliveryInUSD: Double,
                                          UnderdeliveryInUSD: Double,
                                          TotalAdvertiserCostFromPerformanceReportInUSD: Double,
                                          EstimatedBudgetInUSD: Double,
                                          UnderdeliveryFraction: Double
                                        )