package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

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

case class DailyBidRequestDataset(experimentName: String = "") extends KongMingDataset[DailyBidRequestRecord](
  s3DatasetPath = "dailybidrequest/v=1",
  experimentName = config.getString("ttd.DailyBidRequestDataset.experimentName", experimentName)
)
