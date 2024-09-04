package com.thetradedesk.kongming.datasets

final case class AdGroupPolicyMappingRecord(ConfigKey: String,
                                            ConfigValue: String,
                                            AdGroupId: String,
                                            AdGroupIdInt: Int,
                                            AdGroupIdEncoded: Long,
                                            CampaignId: String,
                                            CampaignIdInt: Int,
                                            CampaignIdEncoded: Long,
                                            AdvertiserId: String,
                                            AdvertiserIdInt: Int,
                                            AdvertiserIdEncoded: Long,
                                           )

case class AdGroupPolicyMappingDataset(experimentOverride: Option[String] = None) extends KongMingDataset[AdGroupPolicyMappingRecord](
  s3DatasetPath = "dailyadgrouppolicymapping/v=1",
  experimentOverride = experimentOverride
)
