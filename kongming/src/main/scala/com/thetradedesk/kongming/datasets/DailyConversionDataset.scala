package com.thetradedesk.kongming.datasets

final case class DailyConversionDataRecord( TrackingTagId: String,
                                            UIID: String,
                                            DataAggKey: String,
                                            DataAggValue: String,
                                            ConversionTime: java.sql.Timestamp,
                                            MonetaryValue: Option[BigDecimal],
                                            MonetaryValueCurrency: Option[String]
                                          )


case class DailyConversionDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyConversionDataRecord](
  s3DatasetPath = "dailyconversion/v=1",
  experimentOverride = experimentOverride
)