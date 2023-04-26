package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class CampaignFlightRecord(
                                       CampaignId: String,
                                       StartDateInclusiveUTC: java.sql.Timestamp,
                                       EndDateExclusiveUTC: java.sql.Timestamp,
                                       IsDeleted: Boolean
                                     )

case class CampaignFlightDataSet() extends ProvisioningS3DataSet[CampaignFlightRecord]("campaignflight/v=1", mergeSchema = true){}