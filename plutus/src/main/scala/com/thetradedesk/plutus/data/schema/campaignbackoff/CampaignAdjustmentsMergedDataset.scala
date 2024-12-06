package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Timestamp
import java.time.LocalDate

case class CampaignAdjustmentsMergedDataset (
  CampaignId: String,
  CampaignFlightId: Option[Long],
  CampaignPCAdjustment: Double,

 // Campaign Backoff
  CampaignBackoff_PCAdjustment: Double,
  AddedDate: Option[LocalDate],
  EndDateExclusiveUTC: Timestamp,
  IsValuePacing: Option[Boolean],
  Pacing: Option[Int],
  ImprovedNotPacing: Option[Int],
  WorseNotPacing: Option[Int],
  MinCalculatedCampaignCapInUSD: Double,
  MaxCalculatedCampaignCapInUSD: Double,
  OverdeliveryInUSD: Double,
  UnderdeliveryInUSD: Double,
  TotalAdvertiserCostFromPerformanceReportInUSD: Double,
  EstimatedBudgetInUSD: Double,
  UnderdeliveryFraction: Double,

  // Hades Backoff
  HadesBackoff_PCAdjustment: Double,
  Hades_isProblemCampaign: Boolean,
  BBFPC_OptOut_ShareOfBids: Double,
  BBFPC_OptOut_ShareOfBidAmount: Double,
  IsTest: Option[Boolean]
)

object CampaignAdjustmentsMergedDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusbackoff/campaignadjustmentspacing/v=${DATA_VERSION}"
  // use for write
  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"

  // use for read
  def S3_PATH_DATE_GEN = (date: LocalDate) => {
    f"/date=${paddedDatePart(date)}"
  }
}