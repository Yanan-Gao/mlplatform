package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{campaignUnderdeliveryMock, platformReportMock}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.plutus.data.schema.campaignbackoff.CampaignFloorBufferSchema
import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionTransform.{CampaignUnderDeliveryData, FlightData, OnePercentFloorBufferCriteria, OnePercentFloorBufferRollbackCriteria, PacingData, SupplyVendorData, generateBbfFloorBufferMetrics, generateTodaysSnapshot, getTodays1PercentFloorBufferCandidateCampaigns, getTodays1PercentRollbackCampaigns}

import java.sql.Timestamp
import java.time.LocalDateTime

class CampaignFloorBufferCandidateSelectionTransformTest extends TTDSparkTest {

  val testDate = LocalDateTime.of(2025, 4, 22, 12, 33).toLocalDate

  test("Testing CampaignFloorBufferCandidateSelection generateTodaysSnapshot") {
    // Test1 -> Latest buffer floor snapshot exists and new candidates generated today
    val latestBufferFloorSnapshot = Seq(
      CampaignFloorBufferSchema("abc1", 0.2, testDate.minusDays(3)),
      CampaignFloorBufferSchema("abc2", 0.1, testDate.minusDays(2)),
      CampaignFloorBufferSchema("abc3", 0.1, testDate),
    ).toDS()
    val todaysCandidateCampaignsWithBuffer = Seq(
      CampaignFloorBufferSchema("abc4", 0.2, testDate),
      CampaignFloorBufferSchema("abc2", 0.2, testDate),
    ).toDS()

    val testTodaysSnapshot1 = generateTodaysSnapshot(latestBufferFloorSnapshot, todaysCandidateCampaignsWithBuffer, Seq.empty[CampaignFloorBufferSchema].toDS())

    assert(testTodaysSnapshot1.collectAsList().size() == 4, "Validating snapshot size")

    // If the campaign exists in today's candidates and latest snapshot, today's buffer value will be used.
    val overridenCampaign = testTodaysSnapshot1.filter($"CampaignId" === "abc2").collect().head
    assert(overridenCampaign.BBF_FloorBuffer == 0.2, "Campaign used today's floor buffer")
    assert(overridenCampaign.AddedDate == testDate, "Campaign date is updated")

    // Check the snapshot has correct dates
    val oldCampaign = testTodaysSnapshot1.filter($"CampaignId" === "abc1").collect().head
    assert(oldCampaign.AddedDate == testDate.minusDays(3), "Campaign has correct Added date")

    // Test 2 -> Campaigns found in rollback dataset should be removed from today's snapshot
    val todaysRollbackEligibleCampaigns = Seq(
      CampaignFloorBufferSchema("abc1", 0.2, testDate),
    ).toDS()
    val testTodaysSnapshot2 = generateTodaysSnapshot(latestBufferFloorSnapshot, todaysCandidateCampaignsWithBuffer, todaysRollbackEligibleCampaigns)
    assert(testTodaysSnapshot2.collectAsList().size() == 3, "Validating snapshot size after rollback")
    assert(testTodaysSnapshot2.filter($"CampaignId" === "abc1").isEmpty)
  }

  test("Testing CampaignFloorBufferCandidateSelection getTodays1PercentBufferFloorCandidateCampaigns") {
    val onePercentFloorBufferCriteria = OnePercentFloorBufferCriteria(
      avgUnderdeliveryFraction=0.02,
      avgCampaignThrottleMetric=0.8,
      openMarketShare=0.01,
      openPathShare=0.01,
      floorBuffer = 0.01
    )
    val mockPlatformReportData = platformReportMock(privateContractId = null)
    val mockSupplyVendorData = Seq(SupplyVendorData("supplyvendor", OpenPathEnabled = true)).toDS()
    val mockFlightData = Seq(FlightData("newcampaign1", Timestamp.valueOf(LocalDateTime.of(2025, 4, 26, 13, 20)))).toDS()
    val mockPacingData = Seq(PacingData("newcampaign1", true)).toDS()
    val campaignUnderDeliveryData = Seq(CampaignUnderDeliveryData("newcampaign1", 0.02, 0.02, 0.8, 0.01)).toDS()
    val latestRollbackBufferSnapshot = Seq(CampaignFloorBufferSchema("oldcampaign", 0.6, testDate)).toDS()

    // Test1 -> Campaign is added to todays candidates if meets the criteria
    val todaysTestCampaigns1 = getTodays1PercentFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      pacingData = mockPacingData,
      campaignUnderDeliveryData = campaignUnderDeliveryData,
      latestRollbackBufferSnapshot = latestRollbackBufferSnapshot,
      testSplit = Some(0.9)
    )

    assert(todaysTestCampaigns1.collectAsList().size() == 1)

    // Test2 -> Campaign is NOT added to todays candidates if underdelivery fraction >=0.2 and Throttle > 0.8
    val todaysTestCampaignsUnderDeliveryFractionAndThrottleCriteria = getTodays1PercentFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      pacingData = mockPacingData,
      campaignUnderDeliveryData = Seq(CampaignUnderDeliveryData("newcampaign1", 0.02, 0.02, 0.9, 0.01)).toDS(),
      latestRollbackBufferSnapshot = latestRollbackBufferSnapshot,
      testSplit = Some(0.9)
    )

    assert(todaysTestCampaignsUnderDeliveryFractionAndThrottleCriteria.collectAsList().size() == 0)

    // Test3 -> Campaign is added to todays candidates if the rollback date is > 7 days
    val todaysTestCampaignsRollbackDatePriorTo7Days = getTodays1PercentFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      pacingData = mockPacingData,
      campaignUnderDeliveryData = campaignUnderDeliveryData,
      latestRollbackBufferSnapshot = Seq(CampaignFloorBufferSchema("newcampaign1", 0.6, testDate.minusDays(7))).toDS(),
      testSplit = Some(0.9)
    )

    assert(todaysTestCampaignsRollbackDatePriorTo7Days.collectAsList().size() == 1)

  }

  test("Testing CampaignFloorBufferCandidateSelection getTodays1PercentRollbackCampaigns") {
    val latestCampaignFloorBufferSnapshot = Seq(
      CampaignFloorBufferSchema("abc1", 0.01, testDate),
      CampaignFloorBufferSchema("abc2", 0.01, testDate.minusDays(1))
    ).toDS()

    val testOnePercentBufferRollbackCriteria = OnePercentFloorBufferRollbackCriteria(
      rollbackUnderdeliveryFraction=0.05,
      rollbackCampaignThrottleMetric=0.8,
    )

    // Test 1 -> Rollback candidates found
    val todaysOnePercentCampaignToRollBack = getTodays1PercentRollbackCampaigns(
      testDate,
      latestCampaignFloorBufferSnapshot,
      campaignUnderdeliveryMock(
        campaignId = "abc2",
        date = Timestamp.valueOf(LocalDateTime.of(2025, 4, 22, 0, 0)),
        fraction = 0.06,
        throttle = 0.9
      ),
      testOnePercentBufferRollbackCriteria
    )

    assert(todaysOnePercentCampaignToRollBack.collectAsList().size() == 1)

    // Test 2 -> Rollback candidates not found
    val todaysCampaignToRollBackNotFound = getTodays1PercentRollbackCampaigns(
      testDate,
      latestCampaignFloorBufferSnapshot,
      campaignUnderdeliveryMock(
        campaignId = "abc2",
        date = Timestamp.valueOf(LocalDateTime.of(2025, 4, 22, 0, 0)),
        fraction = 0.03
      ),
      testOnePercentBufferRollbackCriteria
    )

    assert(todaysCampaignToRollBackNotFound.collectAsList().size() == 0)
  }

  test("Testing CampaignFloorBufferCandidateSelection generateBbfFloorBufferMetrics") {
    val todaysCampaignFloorBufferSnapshot = Seq(
      CampaignFloorBufferSchema("abc1", 0.2, testDate),
      CampaignFloorBufferSchema("abc2", 0.1, testDate),
      CampaignFloorBufferSchema("abc3", 0.1, testDate.minusDays(1))
    ).toDS()

    val todaysCampaignFloorBufferRollbackSnapshot = Seq(
      CampaignFloorBufferSchema("xyz1", 0.2, testDate),
    ).toDS()

    val testMetrics = generateBbfFloorBufferMetrics(todaysCampaignFloorBufferSnapshot, todaysCampaignFloorBufferRollbackSnapshot)

    assert(testMetrics.floorBufferTotalCount == 3)
    assert(testMetrics.floorBufferRollbackTotalCount == 1)

    val expectedBufferFloorCounts = Map(0.2 -> 1, 0.1 -> 2)
    val resultCounts = testMetrics.countByFloorBuffer.collect().map(row => row.getDouble(0) -> row.getLong(1)).toMap
    assert(resultCounts == expectedBufferFloorCounts, s"Expected $expectedBufferFloorCounts but got $resultCounts")
  }
}
