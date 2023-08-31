package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.util.TTDConfig.config

final case class DailyExchangeRateRecord(
                                          CurrencyCodeId: String,
                                          FromUSD: BigDecimal,
                                          AsOfDateUTC: java.sql.Timestamp
                                        )

case class CurrencyExchangeRateDataSet() extends ProvisioningS3DataSet[DailyExchangeRateRecord]("currencyexchangerate/v=1", true) {}

case class DailyExchangeRateDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyExchangeRateRecord](
  s3DatasetPath = "dailyexchangerate/v=1",
  experimentOverride = experimentOverride
)
