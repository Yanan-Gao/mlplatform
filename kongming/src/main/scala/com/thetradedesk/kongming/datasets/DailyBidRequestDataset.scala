package com.thetradedesk.kongming.datasets

case class BidRequestPolicyRecord( UIID: String,
                                   ConfigKey: String,
                                   ConfigValue: String,
                                   DataAggKey: String,
                                   DataAggValue: String,
                                   BidRequestId: String,
                                   LogEntryTime: java.sql.Timestamp,
                                   IsImp: Boolean )

final case class DailyBidRequestRecord(UIID: String,
                                       ConfigKey: String,
                                       ConfigValue: String,
                                       DataAggKey: String,
                                       DataAggValue: String,
                                       BidRequestId: String,
                                       LogEntryTime: java.sql.Timestamp,
                                       IsImp: Boolean,
                                       RecencyRank: Int)

case class DailyBidRequestDataset() extends KongMingDataset[DailyBidRequestRecord](
  s3DatasetPath = "dailybidrequest/v=1"
)
