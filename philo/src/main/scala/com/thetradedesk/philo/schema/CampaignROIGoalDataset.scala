package com.thetradedesk.philo.schema

case class CampaignROIGoalRecord(CampaignId: String, ROIGoalTypeId: String)

object CampaignROIGoalDataset {
  val CAMPAIGNROIGOALS3: String = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignroigoal/v=1"
}
