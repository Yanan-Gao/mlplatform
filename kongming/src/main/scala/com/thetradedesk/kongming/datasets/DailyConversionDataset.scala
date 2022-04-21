package com.thetradedesk.kongming.datasets

import org.apache.spark.sql.{Dataset, SaveMode}
import java.time.LocalDate

final case class DailyConversionDataRecord( TrackingTagId: String,
                                            UIID: String,
                                            DataAggKey: String,
                                            DataAggValue: String,
                                            ConversionTime: java.sql.Timestamp
                                          )


object DailyConversionDataset extends KongMingDataset[DailyConversionDataRecord](
  s3DatasetPath = "dailyconversion/v=1",
  defaultNumPartitions = 100
)