package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import java.sql.Timestamp

case class CampaignRecord(CampaignId: String,
                          PartnerId: String,
                          StartDate: Timestamp,
                          EndDate: Timestamp,
                          CustomCPATypeId: Int)

case class CampaignDataSet() extends
  ProvisioningS3DataSet[CampaignRecord]("campaign/v=1")