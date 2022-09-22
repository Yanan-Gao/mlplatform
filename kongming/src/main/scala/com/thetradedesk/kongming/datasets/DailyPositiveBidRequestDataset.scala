package com.thetradedesk.kongming.datasets

final case class DailyPositiveLabelRecord(
                                           ConfigKey: String,
                                           ConfigValue: String,
                                           DataAggKey: String,
                                           DataAggValue: String,
                                           BidRequestId: String,
                                           TrackingTagId: String,
                                           UIID: String,
                                           ConversionTime: java.sql.Timestamp,
                                           LogEntryTime: java.sql.Timestamp,
                                           IsImp: Boolean,
                                           IsClickWindowGreater: Boolean,
                                           IsInClickAttributionWindow: Boolean,
                                           IsInViewAttributionWindow: Boolean
                                         )

case class DailyPositiveBidRequestDataset() extends KongMingDataset[DailyPositiveLabelRecord](
  s3DatasetPath = "dailypositive/v=1"
)