package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.sql.Timestamp
import java.time.LocalDate

case class MergedCampaignAdjustmentsSchema(
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
  CampaignType: Option[String],
  CampaignType_Yesterday: Option[String],
)

object MergedCampaignAdjustmentsDataset extends S3DailyParquetDataset[MergedCampaignAdjustmentsSchema]{
  val DATA_VERSION = 1

  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignadjustmentspacing/v=${DATA_VERSION}"
}