package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class AdGroupRecord(AdGroupId: String,
                         CampaignId: String,
                         IsEnabled: Boolean
                        )

case class AdGroupDataSet() extends
  ProvisioningS3DataSet[AdGroupRecord]("adgroup/v=1")