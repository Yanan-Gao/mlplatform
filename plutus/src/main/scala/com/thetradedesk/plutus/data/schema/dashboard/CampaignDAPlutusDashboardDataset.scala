package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Date
import java.time.LocalDate

case class CampaignDAPlutusDashboardSchema(
                                              Date: Date,
                                              CampaignId: String,
                                              AdGroupId: String,
                                              Channel: String,
                                              //MarketType: String,
                                              DetailedMarketType: String,
                                              PredictiveClearingEnabled: Boolean,
                                              AggPredictiveClearingEnabled: Boolean,
                                              PredictiveClearingMode: Int,
                                              IsValuePacing: Option[Boolean],
                                              //PushdownDial: Int,

                                              ImpressionCount: Long,
                                              BidCount: Long,
                                              MediaCostCPMInUSD: Double,
                                              FeeAmount: Option[Double],
                                              PartnerCostInUSD: Double,
                                              AdvertiserCostInUSD: Double,
                                              InitialBid: Double,
                                              FirstPriceAdjustment: Option[Double],
                                              FinalBid: Double,
                                              Avg_PushdownDial: Option[Double]

  )

object CampaignDAPlutusDashboardDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusdashboard/campaign_da/v=${DATA_VERSION}"
  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"

}