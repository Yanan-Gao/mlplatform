package com.thetradedesk.kongming.datasets

final case class CampaignRecord(CampaignId: String,
                                AdvertiserId: String,
                                CustomCPATypeId: Int,
                                CustomCPAClickWeight: Option[BigDecimal],
                                CustomCPAViewthroughWeight: Option[BigDecimal]
                               )

object CampaignDataset {
  val S3Path = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaign/v=1/"
}