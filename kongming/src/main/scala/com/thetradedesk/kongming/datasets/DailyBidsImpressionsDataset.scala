package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema

case class DailyBidsImpressionsDataset() extends KongMingDataset[BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1"
)
