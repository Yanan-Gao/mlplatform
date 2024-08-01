package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.core.SchemaPolicy.MergeAllFilesSchema

final case class PartnerGroupRecord(
                               PartnerGroupId: String,
                               TenantId: String
                               )

case class PartnerGroupDataSet() extends ProvisioningS3DataSet[PartnerGroupRecord](
  "partnergroup/v=1",
  schemaPolicy = MergeAllFilesSchema
)
