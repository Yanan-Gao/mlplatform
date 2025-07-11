package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.core.SchemaPolicy.MergeAllFilesSchema

final case class PartnerRecord(PartnerId: String,
                               PartnerGroupId: String,
                                SpendDisabled: Boolean
                               )

case class PartnerDataSet() extends ProvisioningS3DataSet[PartnerRecord](
  "partner/v=1",
  schemaPolicy = MergeAllFilesSchema
)
