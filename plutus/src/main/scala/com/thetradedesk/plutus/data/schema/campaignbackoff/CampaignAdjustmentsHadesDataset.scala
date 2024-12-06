package com.thetradedesk.plutus.data.schema.campaignbackoff

case class CampaignAdjustmentsHadesSchema (
  CampaignId: String,
  HadesBackoff_PCAdjustment: Option[Double],
  Hades_isProblemCampaign: Boolean,
  BBFPC_OptOut_ShareOfBids: Option[Double],
  BBFPC_OptOut_ShareOfBidAmount: Option[Double]
)
