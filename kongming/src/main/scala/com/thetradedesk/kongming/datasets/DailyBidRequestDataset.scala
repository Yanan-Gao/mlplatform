package com.thetradedesk.kongming.datasets

final case class DailyBidRequestRecord(UIID: String,
                                       ConfigKey: String,
                                       ConfigValue: String,
                                       DataAggKey: String,
                                       DataAggValue: String,
                                       BidRequestId: String,
                                       LogEntryTime: java.sql.Timestamp,
                                       RecencyRank: Int)

object DailyBidRequestDataset extends KongMingDataset[DailyBidRequestRecord](
  s3DatasetPath = "dailybidrequest/v=1",
  defaultNumPartitions = 100
)
