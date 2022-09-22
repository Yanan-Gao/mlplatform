package com.thetradedesk.kongming.datasets

final case class OfflineScoredImpressionRecord(
                                              BidRequestId: String,
                                              AdGroupId: String,
                                              Score: Double
                                              )


case class OfflineScoredImpressionDataset() extends KongMingDataset[OfflineScoredImpressionRecord](
  s3DatasetPath = "measurement/offline/v=1",
  partitionField = "model_date"
)