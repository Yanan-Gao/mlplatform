package com.thetradedesk.kongming.datasets

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode

import java.time.LocalDate

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

object DailyPositiveBidRequestDataset extends KongMingDataset[DailyPositiveLabelRecord](
  s3DatasetPath = "dailypositive/v=1",
  defaultNumPartitions = 100
)