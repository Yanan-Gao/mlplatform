package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import java.sql.Timestamp

case class ExchangeRateRecord(CurrencyCodeId: String,
                              FromUSD: Double,
                              AsOfDateUtc: Timestamp
                           )

case class ExchangeRateDataSet() extends
  ProvisioningS3DataSet[ExchangeRateRecord]("currencyexchangerate/v=1")