package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

case class CampaignAdjustmentsSchema(
                                      CampaignId: String,
                                      CampaignPCAdjustment: Double,
                                      CampaignBbfFloorBuffer: Double
                                    )

object CampaignAdjustmentsDataset extends S3DailyParquetDataset[CampaignAdjustmentsSchema]{
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String =
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignadjustments/v=${DATA_VERSION}"
}