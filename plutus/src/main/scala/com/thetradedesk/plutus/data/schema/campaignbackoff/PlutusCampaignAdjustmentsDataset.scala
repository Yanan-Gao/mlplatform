package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.sql.Timestamp
import java.time.LocalDate

case class CampaignAdjustmentsPacingSchema(
                                      CampaignId: String,
                                      CampaignFlightId: Option[Long],
                                      CampaignPCAdjustment: Double,
                                      IsTest: Option[Boolean],
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
                                      UnderdeliveryFraction: Double
                                    )



object PlutusCampaignAdjustmentsDataset extends S3DailyParquetDataset[CampaignAdjustmentsPacingSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/plutusadjustments/v=${DATA_VERSION}"
  }
}
