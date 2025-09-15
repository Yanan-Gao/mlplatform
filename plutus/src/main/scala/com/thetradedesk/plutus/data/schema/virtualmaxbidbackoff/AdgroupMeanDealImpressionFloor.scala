package com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

case class AdgroupDealImpMeanFloorDataset(
                                      AdgroupId: String,
                                      MeanDealImpressionFloor: Double)

object AdgroupMeanDealImpressionFloorDataset extends S3DailyParquetDataset[AdgroupDealImpMeanFloorDataset]{
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String =
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/adgroup_predictive_clearing_metadata/v=${DATA_VERSION}"
}

