package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataframe

object MergedCampaignAdjustmentsDataset extends S3DailyParquetDataframe {
  val DATA_VERSION = 1

  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignadjustmentspacing/v=${DATA_VERSION}"
}