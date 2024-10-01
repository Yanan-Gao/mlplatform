package com.thetradedesk.plutus.data.mockdata

import MockData.{platformReportMock, _}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsPacingSchema, CampaignThrottleMetricSchema, CampaignFlightRecord, RtbPlatformReportCondensedData}
import com.thetradedesk.spark.datasets.sources.CountryRecord
import org.apache.spark.sql.Dataset

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

object DataGenerator {

  def generatePlatformReportData: Dataset[RtbPlatformReportCondensedData] = {
    val platformReportDataSeq = Seq(
      platformReportMock(),
      platformReportMock(campaignId=Some("newcampaign2"), pcsavings=Some(4.0)),
      platformReportMock(campaignId=Some("newcampaign3"), pcsavings=Some(6.0)),
      platformReportMock(campaignId=Some("newcampaign4"), pcsavings=Some(6.0)),
      platformReportMock(campaignId=Some("newcampaign5"), imps=Some(210000), pcsavings=Some(2.0)), // underdelivering but not due to plutus
      platformReportMock(country=Some("Italy"), campaignId=Some("testgroup"), imps=Some(75000)),
      platformReportMock(campaignId=Some("campaign4"), pcsavings=Some(4.0)),
      platformReportMock(campaignId=Some("campaign2"), pcsavings=Some(6.0)),
      platformReportMock(campaignId=Some("campaign5"), pcsavings=Some(3.0)),
      platformReportMock(campaignId=Some("ccampaign4"), pcsavings=Some(4.0)),
      platformReportMock(campaignId=Some("ccampaign5"), pcsavings=Some(6.0))
    )
    platformReportDataSeq.reduce(_ union _)
  }

  def generateCountryData: Dataset[CountryRecord] = {
    val countryDataSeq = Seq(
      countryMock(),
      countryMock(countryid = "xidhxke6my", short = "IT", long = "Italy", continentalregionid = Some(2))
    )
    countryDataSeq.reduce(_ union _)
  }

  def generateCampaignUnderdeliveryData: Dataset[CampaignThrottleMetricSchema] = {
    val campaignUnderdeliveryDataSeq = Seq(
      // new campaign newcampaign1 that is underdelivering 4/5 days
      campaignUnderdeliveryMock(),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), underdelivery = 0, spend = 4500, fraction = 0),
      // new campaign newcampaign2 that is underdelivering 3/5 days (use to check isControl split)
      campaignUnderdeliveryMock(campaignId = "newcampaign2", campaignFlightId = 3344550),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "newcampaign2", campaignFlightId = 3344550, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "newcampaign2", campaignFlightId = 3344550, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "newcampaign2", campaignFlightId = 3344550, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "newcampaign2", campaignFlightId = 3344550, underdelivery = 0, spend = 4500, fraction = 0),
      // new campaign newcampaign3 that is underdelivering 5/5 days (use to check isControl split)
      campaignUnderdeliveryMock(campaignId = "newcampaign3", campaignFlightId = 5566770),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "newcampaign3", campaignFlightId = 5566770, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "newcampaign3", campaignFlightId = 5566770, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "newcampaign3", campaignFlightId = 5566770, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "newcampaign3", campaignFlightId = 5566770, underdelivery = 4000, spend = 500, fraction = 0.88),
      // new campaign newcampaign4 that is underdelivering 5/5 days (use to check isControl split)
      campaignUnderdeliveryMock(campaignId = "newcampaign4", campaignFlightId = 7788990),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "newcampaign4", campaignFlightId = 7788990, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "newcampaign4", campaignFlightId = 7788990, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "newcampaign4", campaignFlightId = 7788990, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "newcampaign4", campaignFlightId = 7788990, underdelivery = 4000, spend = 500, fraction = 0.88),
      // new campaign newcampaign5 that is underdelivering 5/5 days (but eventually not due to pc)
      campaignUnderdeliveryMock(campaignId = "newcampaign5", campaignFlightId = 1231231, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "newcampaign5", campaignFlightId = 1231231, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "newcampaign5", campaignFlightId = 1231231, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "newcampaign5", campaignFlightId = 1231231, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "newcampaign5", campaignFlightId = 1231231, underdelivery = 4000, spend = 500, fraction = 0.88),
      // new campaign newcampaign6 that is not underdelivering
      campaignUnderdeliveryMock(campaignId = "newcampaign6", campaignFlightId = 4564564, underdelivery = 0, spend = 4500, fraction = 0),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "newcampaign6", campaignFlightId = 4564564, underdelivery = 0, spend = 4500, fraction = 0),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "newcampaign6", campaignFlightId = 4564564, underdelivery = 10, spend = 4490, fraction = 0.002),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "newcampaign6", campaignFlightId = 4564564, underdelivery = 10, spend = 4490, fraction = 0.002),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "newcampaign6", campaignFlightId = 4564564, underdelivery = 0, spend = 4500, fraction = 0),

      // existing campaign1 that was previously adjusted, had been pacing for prev day and continuing to pace
      campaignUnderdeliveryMock(campaignId = "campaign1", campaignFlightId = 1000000, underdelivery = 45, spend = 4140, fraction = 0.01),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign1", campaignFlightId = 1000000, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign1", campaignFlightId = 1000000, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign1", campaignFlightId = 1000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign1", campaignFlightId = 1000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      // existing campaign2 that was previously adjusted, had been pacing for prev day and continuing to pace, but flight has just ended
      campaignUnderdeliveryMock(campaignId = "campaign2", campaignFlightId = 2000001, underdelivery = 45, spend = 4140, fraction = 0.01),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign2", campaignFlightId = 2000000, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign2", campaignFlightId = 2000000, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign2", campaignFlightId = 2000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign2", campaignFlightId = 2000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      // existing campaign3 that was previously adjusted, had been improving-not-pacing for prev day and continuing to improve-not-pace at major rate, and its flight enddate was changed mid-flight
      campaignUnderdeliveryMock(campaignId = "campaign3", campaignFlightId = 3000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign3", campaignFlightId = 3000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign3", campaignFlightId = 3000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign3", campaignFlightId = 3000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign3", campaignFlightId = 3000000, underdelivery = 4500, spend = 0, fraction = 1),
      // existing campaign4 that was previously adjusted, had been worse-not-pacing for prev day and continuing to worse-not-pace
      campaignUnderdeliveryMock(campaignId = "campaign4", campaignFlightId = 4000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign4", campaignFlightId = 4000000),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign4", campaignFlightId = 4000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign4", campaignFlightId = 4000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign4", campaignFlightId = 4000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      // existing campaign5 that was previously adjusted, had been worse-not-pacing for prev day and continuing to worse-not-pace, and flight is ending soon
      campaignUnderdeliveryMock(campaignId = "campaign5", campaignFlightId = 5000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign5", campaignFlightId = 5000000),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign5", campaignFlightId = 5000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign5", campaignFlightId = 5000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign5", campaignFlightId = 5000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      // existing campaign6 that was previously adjusted, had been worse-not-pacing for prev 3 days and continuing to worse-not-pace
      campaignUnderdeliveryMock(campaignId = "campaign6", campaignFlightId = 6000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign6", campaignFlightId = 6000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign6", campaignFlightId = 6000000),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign6", campaignFlightId = 6000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign6", campaignFlightId = 6000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      // existing campaign7 that was previously adjusted, continuing to worse-not-pace, but hitting min adjustment cap. Also checking for pacing percent change calculation when previous day's pacing fraction = 0
      campaignUnderdeliveryMock(campaignId = "campaign7", campaignFlightId = 7000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign7", campaignFlightId = 7000000, underdelivery = 0, spend = 4500, fraction = 0),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign7", campaignFlightId = 7000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign7", campaignFlightId = 7000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign7", campaignFlightId = 7000000),
      // existing campaign8 that was previously adjusted and had its adjustment reset previous day
      campaignUnderdeliveryMock(campaignId = "campaign8", campaignFlightId = 8000000),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign8", campaignFlightId = 8000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign8", campaignFlightId = 8000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign8", campaignFlightId = 8000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign8", campaignFlightId = 8000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      // existing campaign9 that was previously adjusted, had been improving-not-pacing for prev day and continuing to improve-not-pace at minor rate
      campaignUnderdeliveryMock(campaignId = "campaign9", campaignFlightId = 9000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaign9", campaignFlightId = 9000000, underdelivery = 2700, spend = 1800, fraction = 0.6),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaign9", campaignFlightId = 9000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaign9", campaignFlightId = 9000000, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaign9", campaignFlightId = 9000000, underdelivery = 4500, spend = 0, fraction = 1),
      // existing campaign10 that was previously adjusted, had been improving-not-pacing for prev day and continuing to improve-not-pace at minor rate, and flight is ending soon
      campaignUnderdeliveryMock(campaignId = "campaignn10", campaignFlightId = 10000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaignn10", campaignFlightId = 10000000, underdelivery = 2700, spend = 1800, fraction = 0.6),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaignn10", campaignFlightId = 10000000, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaignn10", campaignFlightId = 10000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaignn10", campaignFlightId = 10000000),
      // existing campaign11 that was previously adjusted, had been improving-not-pacing for prev day and switched to worse-not-pace
      campaignUnderdeliveryMock(campaignId = "campaignn11", campaignFlightId = 11000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "campaignn11", campaignFlightId = 11000000, underdelivery = 0, spend = 4500, fraction = 0),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "campaignn11", campaignFlightId = 11000000, underdelivery = 2250, spend = 2250, fraction = 0.5),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "campaignn11", campaignFlightId = 11000000, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "campaignn11", campaignFlightId = 11000000),

      // Control campaigns
      campaignUnderdeliveryMock(campaignId = "ccampaign1", campaignFlightId = 1111111, underdelivery = 360, spend = 4140, fraction = 0.08),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "ccampaign1", campaignFlightId = 1111111, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "ccampaign1", campaignFlightId = 1111111, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "ccampaign1", campaignFlightId = 1111111, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "ccampaign1", campaignFlightId = 1111111, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(campaignId = "ccampaign2", campaignFlightId = 2222221, underdelivery = 360, spend = 4140, fraction = 0.08),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "ccampaign2", campaignFlightId = 2222222, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "ccampaign2", campaignFlightId = 2222222, underdelivery = 225, spend = 4275, fraction = 0.05),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "ccampaign2", campaignFlightId = 2222222, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "ccampaign2", campaignFlightId = 2222222, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(campaignId = "ccampaign3", campaignFlightId = 3333333, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "ccampaign3", campaignFlightId = 3333333, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "ccampaign3", campaignFlightId = 3333333, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "ccampaign3", campaignFlightId = 3333333, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "ccampaign3", campaignFlightId = 3333333, underdelivery = 4500, spend = 0, fraction = 1),
      campaignUnderdeliveryMock(campaignId = "ccampaign4", campaignFlightId = 4444444, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "ccampaign4", campaignFlightId = 4444444),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "ccampaign4", campaignFlightId = 4444444, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "ccampaign4", campaignFlightId = 4444444, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "ccampaign4", campaignFlightId = 4444444, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(campaignId = "ccampaign5", campaignFlightId = 5555555, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-24 00:00:00"), campaignId = "ccampaign5", campaignFlightId = 5555555),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-23 00:00:00"), campaignId = "ccampaign5", campaignFlightId = 5555555, underdelivery = 3500, spend = 2000, fraction = 0.556),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-22 00:00:00"), campaignId = "ccampaign5", campaignFlightId = 5555555, underdelivery = 4000, spend = 500, fraction = 0.88),
      campaignUnderdeliveryMock(date = Timestamp.valueOf("2024-06-21 00:00:00"), campaignId = "ccampaign5", campaignFlightId = 5555555, underdelivery = 4000, spend = 500, fraction = 0.88),
    )
    campaignUnderdeliveryDataSeq.reduce(_ union _)
  }

  def generateCampaignFlightData: Dataset[CampaignFlightRecord] = {
    val campaignFlightDataSeq = Seq(
      campaignFlightMock(),
      campaignFlightMock(campaignFlightId = 3344550L, campaignId = "newcampaign2"),
      campaignFlightMock(campaignFlightId = 5566770L, campaignId = "newcampaign3"),
      campaignFlightMock(campaignFlightId = 7788990L, campaignId = "newcampaign4"),
      campaignFlightMock(campaignFlightId = 1231231L, campaignId = "newcampaign5"),
      campaignFlightMock(campaignFlightId = 4564564L, campaignId = "newcampaign6"),
      campaignFlightMock(campaignFlightId = 1000000L, campaignId = "campaign1"),
      campaignFlightMock(campaignFlightId = 2000000L, campaignId = "campaign2", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 25, 0, 0)), isCurrent = 0),
      campaignFlightMock(campaignFlightId = 2000001L, campaignId = "campaign2", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 7, 30, 0, 0))), // new flight for campaign2 but this info should never get stored
      campaignFlightMock(campaignFlightId = 3000000L, campaignId = "campaign3", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 9, 1, 0, 0))),
      campaignFlightMock(campaignFlightId = 4000000L, campaignId = "campaign4"),
      campaignFlightMock(campaignFlightId = 5000000L, campaignId = "campaign5", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 28, 0, 0))),
      campaignFlightMock(campaignFlightId = 6000000L, campaignId = "campaign6"),
      campaignFlightMock(campaignFlightId = 7000000L, campaignId = "campaign7"),
      campaignFlightMock(campaignFlightId = 8000000L, campaignId = "campaign8"),
      campaignFlightMock(campaignFlightId = 9000000L, campaignId = "campaign9"),
      campaignFlightMock(campaignFlightId = 10000000L, campaignId = "campaignn10", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 28, 0, 0))),
      campaignFlightMock(campaignFlightId = 11000000L, campaignId = "campaignn11"),
      // Control campaigns
      campaignFlightMock(campaignFlightId = 1111111L, campaignId = "ccampaign1"),
      campaignFlightMock(campaignFlightId = 2222222L, campaignId = "ccampaign2", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 25, 0, 0)), isCurrent = 0),
      campaignFlightMock(campaignFlightId = 2222221L, campaignId = "ccampaign2", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 7, 30, 0, 0))),
      campaignFlightMock(campaignFlightId = 3333333L, campaignId = "ccampaign3", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 7, 1, 0, 0))),
      campaignFlightMock(campaignFlightId = 4444444L, campaignId = "ccampaign4"),
      campaignFlightMock(campaignFlightId = 5555555L, campaignId = "ccampaign5", enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 28, 0, 0)))
    )
    campaignFlightDataSeq.reduce(_ union _)
  }

  def generateCampaignAdjustmentsPacingData: Dataset[CampaignAdjustmentsPacingSchema] = {
    val campaignAdjustmentsPacingDataSeq = Seq(
      campaignAdjustmentsPacingMock(),
      campaignAdjustmentsPacingMock(campaignId = "campaign2", campaignFlightId = Some(2000000L)),
      campaignAdjustmentsPacingMock(campaignId = "campaign3", campaignFlightId = Some(3000000L), pacing = Some(0), improvedNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaign4", campaignFlightId = Some(4000000L), pacing = Some(0), worseNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaign5", campaignFlightId = Some(5000000L), enddate = Timestamp.valueOf(LocalDateTime.of(2024, 6, 28, 0, 0)), pacing = Some(0), worseNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaign6", campaignFlightId = Some(6000000L), pacing = Some(0), worseNotPacing = Some(3)),
      campaignAdjustmentsPacingMock(campaignId = "campaign7", campaignFlightId = Some(7000000L), adjustment = 0.2, pacing = Some(0), worseNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaign8", campaignFlightId = Some(8000000L), adjustment = 1, pacing = Some(0), worseNotPacing = Some(4)),
      campaignAdjustmentsPacingMock(campaignId = "campaign9", campaignFlightId = Some(9000000L), pacing = Some(0), improvedNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaignn10", campaignFlightId = Some(10000000L), pacing = Some(0), improvedNotPacing = Some(1)),
      campaignAdjustmentsPacingMock(campaignId = "campaignn11", campaignFlightId = Some(11000000L), pacing = Some(0), improvedNotPacing = Some(1)),
      // control campaigns
      campaignAdjustmentsPacingMock(campaignId = "ccampaign1", campaignFlightId = Some(1111111L),istest=Some(false), adjustment = 1, pacing = Some(0)),
      campaignAdjustmentsPacingMock(campaignId = "ccampaign2", campaignFlightId = Some(2222222L), istest=Some(false), adjustment = 1, pacing = Some(0)),
      campaignAdjustmentsPacingMock(campaignId = "ccampaign3", campaignFlightId = Some(3333333L), istest=Some(false), adjustment = 1, pacing = Some(0)),
      campaignAdjustmentsPacingMock(campaignId = "ccampaign4", campaignFlightId = Some(4444444L), istest=Some(false), adjustment = 1, pacing = Some(0)),
      campaignAdjustmentsPacingMock(campaignId = "ccampaign5", campaignFlightId = Some(5555555L), istest=Some(false), adjustment = 1, pacing = Some(0)),

    )
    campaignAdjustmentsPacingDataSeq.reduce(_ union _)
  }

}
