package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

import java.time.LocalDate

final case class AdGroupPolicyMappingRecord(ConfigKey: String,
                                            ConfigValue: String,
                                            AdGroupId: String,
                                            AdGroupIdInt: Int,
                                            CampaignId: String,
                                            CampaignIdInt: Int,
                                            AdvertiserId: String,
                                            AdvertiserIdInt: Int
                                           )

case class AdGroupPolicyMappingDataset() extends KongMingDataset[AdGroupPolicyMappingRecord](
  s3DatasetPath = "dailyadgrouppolicymapping/v=1"
)
