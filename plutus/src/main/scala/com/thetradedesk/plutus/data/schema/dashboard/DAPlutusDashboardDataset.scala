package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Date
import java.time.LocalDate

case class DAPlutusDashboardSchema(
                                              Date: Date,
                                              Channel: String,
                                              MarketType: String,
                                              DetailedMarketType: String,
                                              PredictiveClearingMode: String,
                                              PredictiveClearingEnabled: Boolean,
                                              IsValuePacing: Option[Boolean],
                                              IsManagedByTTD: Option[Boolean],
                                              ROIGoalTypeName: Option[String],
                                              Bin_InitialBid: Option[Double],
                                              Bin_FirstPriceAdjustment: Option[Double],

                                              FeeAmount: Option[Double],
                                              PartnerCostInUSD: Double,
                                              AdvertiserCostInUSD: Double,
                                              MediaCostCPMInUSD: Double,
                                              InitialBid: Double,
                                              FirstPriceAdjustment: Option[Double],
                                              FloorPrice: Double,
                                              FinalBid: Double,
                                              ImpressionCount: Long,
                                              BidCount: Long,
                                              //TotalBidCount: Long,
                                              bidsAtFloorPlutus: Option[Long],
                                              AvailableSurplus: Option[Double],
                                              finalBidsAtMaxBid: Option[Long]
  )

object DAPlutusDashboardDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusdashboard/platformwide_da/v=${DATA_VERSION}"
  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"
}