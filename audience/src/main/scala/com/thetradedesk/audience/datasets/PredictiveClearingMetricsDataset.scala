package com.thetradedesk.audience.datasets

final case class PredictiveClearingMetricsRecord(CampaignId: String,
                                                 CampaignPCAdjustment: Double,
                                                 CampaignBbfFloorBuffer: Double,
                                                )

object PredictiveClearingMetricsDataset {
  // Platform wide bbf buffer setting
  val PC_PLATFORM_BBF_BUFFER = 0.01
  // If a campaign is not being backed off, there is no adjustment on the PC bid price
  val PC_BACKOFF_NO_ADJUSTMENT = 1
  val PCMetricsS3 = f"s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusbackoff/campaignadjustments/v=1/"
}