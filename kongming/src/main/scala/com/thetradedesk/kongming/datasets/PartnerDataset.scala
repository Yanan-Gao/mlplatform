package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class PartnerRecord(PartnerId: String,
                                SpendDisabled: Boolean
                               )

case class PartnerDataSet() extends ProvisioningS3DataSet[PartnerRecord]("partner/v=1", mergeSchema = true){}
