package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class AdGroupRecord(AdGroupId: String,
                               AdGroupIdInteger: Int,
                               CampaignId: String
                              )

// probably won't need this for bid request filter, layer it in during training data generation
case class AdGroupDataSet() extends ProvisioningS3DataSet[AdGroupRecord]("adgroup/v=1", true) {}
