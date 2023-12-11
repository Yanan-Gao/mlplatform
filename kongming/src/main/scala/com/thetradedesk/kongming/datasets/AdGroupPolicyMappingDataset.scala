package com.thetradedesk.kongming.datasets

final case class AdGroupPolicyMappingRecord(ConfigKey: String,
                                            ConfigValue: String,
                                            AdGroupId: String,
                                            AdGroupIdInt: Int,
                                            CampaignId: String,
                                            CampaignIdInt: Int,
                                            AdvertiserId: String,
                                            AdvertiserIdInt: Int
                                           )

case class AdGroupPolicyMappingDataset(experimentOverride: Option[String] = None) extends KongMingDataset[AdGroupPolicyMappingRecord](
  s3DatasetPath = "dailyadgrouppolicymapping/v=1",
  experimentOverride = experimentOverride
)
