package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class AudienceExtensionSeedSettingsRecord(AdGroupId: String,
                                                     SeedId: BigInt,
                                                     IsInclusive: Boolean,
                                                     Seedtype: Int)

case class AudienceExtensionSeedSettingsDataset() extends
  ProvisioningS3DataSet[AudienceExtensionSeedSettingsRecord]("audienceextensionseedsettings/v=1", true)