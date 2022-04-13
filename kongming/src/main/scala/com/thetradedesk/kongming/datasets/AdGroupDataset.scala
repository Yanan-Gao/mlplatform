package com.thetradedesk.kongming.datasets

final case class AdGroupRecord(AdGroupId: String,
                               AdGroupIdInteger: Int,
                               CampaignId: String
                              )

// probably won't need this for bid request filter, layer it in during training data generation
object AdGroupDataset {
  val ADGROUPS3 = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/adgroup/v=1/"
}
