package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.paddedDatePart

import java.sql.Timestamp
import java.time.LocalDate

case class CampaignAdjustmentsSchema(
                                      CampaignId: String,
                                      CampaignPCAdjustment: Double
                                    )

object CampaignAdjustmentsDataset {
  val DATA_VERSION = 1

  val S3_PATH: String => String = (ttdEnv: String) => f"s3://thetradedesk-mlplatform-us-east-1/env=${ttdEnv}/data/plutusbackoff/campaignadjustments/v=${DATA_VERSION}"

  val S3_PATH_DATE: (LocalDate, String) => String = (date: LocalDate, ttdEnv: String) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"
}