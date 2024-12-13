package com.thetradedesk.plutus.data.schema.campaignbackoff

case class CampaignAdjustmentsHadesSchema (
  CampaignId: String,
  HadesBackoff_PCAdjustment: Double,
  Hades_isProblemCampaign: Boolean,
  BBFPC_OptOut_ShareOfBids: Option[Double],
  BBFPC_OptOut_ShareOfBidAmount: Option[Double],
  HadesBackoff_PCAdjustment_Current: Option[Double],
  HadesBackoff_PCAdjustment_Old: Option[Double],
)
