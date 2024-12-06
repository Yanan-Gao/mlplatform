package com.thetradedesk.plutus.data.schema.campaignbackoff

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
