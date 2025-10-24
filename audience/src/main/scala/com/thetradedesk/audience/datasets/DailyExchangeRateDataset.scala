package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import java.sql.Timestamp


case class DailyExchangeRate(CurrencyCodeId: String,
                             AsOfDateUTC: Timestamp,
                             FromUSD: BigDecimal)



case class DailyExchangeRateDataset() extends
  ProvisioningS3DataSet[DailyExchangeRate]("currencyexchangerate/v=1")