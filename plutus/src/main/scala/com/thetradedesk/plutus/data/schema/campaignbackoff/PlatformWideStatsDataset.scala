package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

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

object PlatformWideStatsDataset extends S3DailyParquetDataset[PlatformWideStatsSchema]{
  val DATA_VERSION = 1

  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/platformwidestats/v=${DATA_VERSION}"
}
