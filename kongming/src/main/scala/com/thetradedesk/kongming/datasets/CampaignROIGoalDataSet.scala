package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.core.SchemaPolicy.MergeAllFilesSchema

final case class CampaignROIGoalRecord(CampaignId: String,
                                       ROIGoalTypeId: Int,
                                       Priority: Int,
                                       ROIGoalValue: Option[BigDecimal]
                               )

case class CampaignROIGoalDataSet() extends ProvisioningS3DataSet[CampaignROIGoalRecord](
  "campaignroigoal/v=1",
  schemaPolicy = MergeAllFilesSchema
)

