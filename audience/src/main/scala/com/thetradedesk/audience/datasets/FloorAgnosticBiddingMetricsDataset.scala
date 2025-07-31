package com.thetradedesk.audience.datasets

final case class FloorAgnosticBiddingMetricsRecord(CampaignId: String,
                                                   CampaignPCAdjustment: Double,
                                                   CampaignBbfFloorBuffer: Double,
                                                   BBF_OptOut_Rate: Double,
                                                   MaxBidMultiplierCap: Double
                                                )

object FloorAgnosticBiddingMetricsDataset {
  // Platform wide bbf buffer setting
  val PC_PLATFORM_BBF_BUFFER = 0.01
  // If a campaign is not being backed off, there is no adjustment on the PC bid price
  val PC_BACKOFF_NO_ADJUSTMENT = 1
  val PC_BACKOFF_NO_OPTOUT = 0
  val MAXBID_MULTIPLIER_DEFAULT = 1.0
  val PCMetricsS3 = f"s3://thetradedesk-mlplatform-us-east-1/env=prod/data/plutusdashboard/hadesbackoffmonitoring/v=1"
}