package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class CampaignPacingSettingsRecord(CampaignId: String,
                                        IsValuePacing: Boolean
                                       )

case class CampaignPacingSettingsDataset() extends
  ProvisioningS3DataSet[CampaignPacingSettingsRecord]("campaignpacingsettings/v=1")
