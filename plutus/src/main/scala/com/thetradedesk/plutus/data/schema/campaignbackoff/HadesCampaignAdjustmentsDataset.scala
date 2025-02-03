package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.time.LocalDate

object HadesCampaignAdjustmentsDataset extends S3DailyParquetDataset[CampaignAdjustmentsHadesSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/hadesadjustments/v=${DATA_VERSION}"
  }
}

case class CampaignAdjustmentsHadesSchema(
  CampaignId: String,
  CampaignType: String,
  HadesBackoff_PCAdjustment: Double,
  Hades_isProblemCampaign: Boolean,
  BBFPC_OptOut_ShareOfBids: Option[Double],
  BBFPC_OptOut_ShareOfBidAmount: Option[Double],
  HadesBackoff_PCAdjustment_Current: Option[Double],
  HadesBackoff_PCAdjustment_Old: Option[Double],
  CampaignType_Yesterday: Option[String],
)
