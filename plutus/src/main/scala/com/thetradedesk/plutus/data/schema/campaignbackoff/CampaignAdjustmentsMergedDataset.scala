package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Timestamp
import java.time.LocalDate

case class CampaignAdjustmentsMergedDataset (
  CampaignId: String,
  CampaignFlightId: Option[Long],
  MergedPCAdjustment: Double,

  // Campaign Backoff
  CampaignPCAdjustment: Option[Double],
  IsTest: Option[Boolean],
  AddedDate: Option[LocalDate],
  EndDateExclusiveUTC: Option[Timestamp],
  IsValuePacing: Option[Boolean],
  Pacing: Option[Int],
  ImprovedNotPacing: Option[Int],
  WorseNotPacing: Option[Int],
  MinCalculatedCampaignCapInUSD: Option[Double],
  MaxCalculatedCampaignCapInUSD: Option[Double],
  OverdeliveryInUSD: Option[Double],
  UnderdeliveryInUSD: Option[Double],
  TotalAdvertiserCostFromPerformanceReportInUSD: Option[Double],
  EstimatedBudgetInUSD: Option[Double],
  UnderdeliveryFraction: Option[Double],

  // Hades Backoff
  HadesBackoff_PCAdjustment: Option[Double],
  Hades_isProblemCampaign: Option[Boolean],
  BBFPC_OptOut_ShareOfBids: Option[Double],
  BBFPC_OptOut_ShareOfBidAmount: Option[Double],
  HadesBackoff_PCAdjustment_Current: Option[Double],
  HadesBackoff_PCAdjustment_Old: Option[Double],
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