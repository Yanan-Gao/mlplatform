package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class CampaignRecord(CampaignId: String,
                                AdvertiserId: String,
                                CustomCPATypeId: Int,
                                CustomCPAClickWeight: Option[BigDecimal],
                                CustomCPAViewthroughWeight: Option[BigDecimal]
                               )

case class CampaignDataSet() extends ProvisioningS3DataSet[CampaignRecord]("campaign/v=1", mergeSchema = true){}
