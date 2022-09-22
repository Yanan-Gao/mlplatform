package com.thetradedesk.kongming.datasets

final case class DailyConversionDataRecord( TrackingTagId: String,
                                            UIID: String,
                                            DataAggKey: String,
                                            DataAggValue: String,
                                            ConversionTime: java.sql.Timestamp
                                          )


case class DailyConversionDataset() extends KongMingDataset[DailyConversionDataRecord](
  s3DatasetPath = "dailyconversion/v=1"
)