package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.GERONIMO_DATA_SOURCE

case class BidsImpressionDataSet() extends
  LightReadableDataset[BidsImpressionsSchema]("/features/data/koav4/v=1/prod/bidsimpressions/", "s3://thetradedesk-mlplatform-us-east-1", source = Some(GERONIMO_DATA_SOURCE))
