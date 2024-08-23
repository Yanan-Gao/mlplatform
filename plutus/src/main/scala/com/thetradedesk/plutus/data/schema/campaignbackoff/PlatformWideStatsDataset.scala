package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart

import java.time.LocalDate

case class PlatformWideStatsSchema(
                                  ContinentalRegionId: Option[Int],
                                  ChannelSimple: String,
                                  Avg_WinRate: Double,
                                  Med_WinRate: Double,
                                  TotalSpend: BigDecimal,
                                  TotalPredictiveClearingSavings: BigDecimal,
                                  Avg_PredictiveClearingSavings: BigDecimal,
                                  Med_PredictiveClearingSavings: BigDecimal,
                                  Avg_FirstPriceAdjustment: BigDecimal,
                                  Med_FirstPriceAdjustment: BigDecimal
                                   )

object PlatformWideStatsDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusbackoff/platformwidestats/v=${DATA_VERSION}"
  // use for write
  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"

  // use for read
  def S3_PATH_DATE_GEN = (date: LocalDate) => {
    f"/date=${paddedDatePart(date)}"
  }
}
