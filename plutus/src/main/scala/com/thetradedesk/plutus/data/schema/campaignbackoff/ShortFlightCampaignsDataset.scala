package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

object ShortFlightCampaignsDataset extends S3DailyParquetDataset[ShortFlightCampaignsSchema]{
  val DATA_VERSION = 1
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/shortflightcampaigns/v=${DATA_VERSION}"
  }
}

case class ShortFlightCampaignsSchema(
                                      CampaignId: String,
                                      MinimumShortFlightFloorBuffer: Double
                                    )