package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

final case class DailyConversionDataRecord( TrackingTagId: String,
                                            UIID: String,
                                            DataAggKey: String,
                                            DataAggValue: String,
                                            ConversionTime: java.sql.Timestamp
                                          )


case class DailyConversionDataset(experimentName: String = "") extends KongMingDataset[DailyConversionDataRecord](
  s3DatasetPath = "dailyconversion/v=1",
  experimentName = config.getString("ttd.DailyConversionDataset.experimentName", experimentName)
)