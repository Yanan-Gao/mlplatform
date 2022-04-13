package com.thetradedesk.kongming.datasets

final case class CampaignRecord(CampaignId: String,
                                CustomCPATypeId: Int
                              )

object CampaignDataset {
  val S3Path = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaign/v=1/"
}