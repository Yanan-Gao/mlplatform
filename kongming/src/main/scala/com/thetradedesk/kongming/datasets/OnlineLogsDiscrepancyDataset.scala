package com.thetradedesk.kongming.datasets

final case class OnlineLogsDiscrepancyRecord(BidRequestId: String,
                                             OnlineFeatures: String,
                                             BidImpressionFeatures: String
                                            )

case class OnlineLogsDiscrepancyDataset(modelName: String) extends KongMingDataset[OnlineLogsDiscrepancyRecord](
  s3DatasetPath = s"onlinelogsdiscrepancy/${modelName}/v=1",
)
