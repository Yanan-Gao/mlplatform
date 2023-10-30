package com.thetradedesk.philo.schema

case class AdGroupRecord(AdGroupId: String, AudienceId: String, IndustryCategoryId: String)

object AdGroupDataset {
val ADGROUPS3: String = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/adgroup/v=1"
}