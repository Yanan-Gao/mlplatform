package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

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

case class DailyPositiveBidRequestDataset(experimentName: String = "") extends KongMingDataset[DailyPositiveLabelRecord](
  s3DatasetPath = "dailypositive/v=1",
  experimentName = config.getString("ttd.DailyPositiveBidRequestDataset.experimentName", "")
)