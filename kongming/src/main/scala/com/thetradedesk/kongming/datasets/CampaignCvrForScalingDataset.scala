package com.thetradedesk.kongming.datasets

final case class CampaignCvrForScalingRecord(
                                                CampaignIdStr: String,
                                                CVR: Double
                                              )

case class CampaignCvrForScalingDataset(experimentOverride: Option[String] = None) extends KongMingDataset[CampaignCvrForScalingRecord](
  s3DatasetPath = "campaigncvrforscaling/v=1",
  experimentOverride = experimentOverride
)