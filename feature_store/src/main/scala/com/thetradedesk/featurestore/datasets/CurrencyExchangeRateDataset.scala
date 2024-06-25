package com.thetradedesk.featurestore.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class DailyExchangeRateRecord(
                                          CurrencyCodeId: String,
                                          FromUSD: BigDecimal,
                                          AsOfDateUTC: java.sql.Timestamp
                                        )

case class CurrencyExchangeRateDataSet() extends ProvisioningS3DataSet[DailyExchangeRateRecord]("currencyexchangerate/v=1", true) {}
