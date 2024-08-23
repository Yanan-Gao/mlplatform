package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class AdGroupRecord(AdGroupId: String,
                         AdGroupIdInteger: Int,
                         CampaignId: String,
                         IsEnabled: Boolean,
                         TargetHighValueUsers: Boolean,
                         ROIGoalTypeId: Int
                        )

case class AdGroupDataSet() extends
  ProvisioningS3DataSet[AdGroupRecord]("adgroup/v=1")