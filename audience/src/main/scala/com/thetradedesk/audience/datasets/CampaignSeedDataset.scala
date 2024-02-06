package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import java.sql.Timestamp

final case class CampaignSeedRecord(CampaignId: String,
                              SeedId: String,
                              LastUpdatedAt: Timestamp)


case class CampaignSeedDataset() extends
  ProvisioningS3DataSet[CampaignSeedRecord]("campaignseed/v=1", true)
