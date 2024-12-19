package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.{envForRead, paddedDatePart}

import java.sql.Timestamp
import java.time.LocalDate

object CampaignThrottleMetricDataset {
  val S3PATH = f"s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=${envForRead}/aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_parquet/"

  def S3PATH_DATE_GEN = (date: LocalDate) => {
    f"date=${paddedDatePart(date)}"
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