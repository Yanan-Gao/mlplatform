package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema

object DailyBidsImpressionsDataset extends KongMingDataset[BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1",
  defaultNumPartitions = 400
)
