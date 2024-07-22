package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Date
import java.time.LocalDate

case class PlutusDashboardSchema(
                                Date: Date,
                                Model: Option[String],
                                Channel: String,
                                SupplyVendor: Option[String],
                                DetailedMarketType: String,
                                MarketType: String,
                                FactorCombination: Option[String],
                                hasParams: Boolean,
                                hasMBTW: Boolean,
                                hasDeal: Boolean,

                                FeeAmount: Option[Double],
                                PartnerCostInUSD: Double,
                                MediaCostCPMInUSD: Double,
                                ImpressionCount: Long,
                                BidCount: Long,
                                //TotalBidCount: Long,
                                bidsAtFloorPlutus: Option[Long],
                                AvailableSurplus: Option[Double],
                                overbid_cpm: Option[Double],
                                spend_cpm: Option[Double],
                                num_overbid: Option[Long],
                                underbid_cpm: Option[Double],
                                non_spend_cpm: Option[Double],
                                num_underbid: Option[Long]
                                )

object PlutusDashboardDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusdashboard/main/v=${DATA_VERSION}"
  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"
}