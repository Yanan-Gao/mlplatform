package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

case class CampaignResetSchema(
                                      CampaignId: String
                                    )

object CampaignResetDataset extends S3DailyParquetDataset[CampaignResetSchema]{
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String =
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignresets/v=${DATA_VERSION}"
}