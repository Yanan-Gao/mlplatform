package com.thetradedesk.frequency.schema

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class CampaignROIGoalRecord(CampaignId: String, ROIGoalTypeId: String, Priority: Integer)

case class CampaignROIGoalDataSet() extends ProvisioningS3DataSet[CampaignROIGoalRecord](
  "campaignroigoal/v=1",
  true
)


