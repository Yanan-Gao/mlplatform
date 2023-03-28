package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.spark.util.TTDConfig.config

case class DailyBidsImpressionsDataset() extends KongMingDataset[BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1",
  experimentName = config.getString("ttd.DailyBidsImpressionsDataset.experimentName", "")
)
