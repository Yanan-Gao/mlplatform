package com.thetradedesk.plutus.data.plutus.transform.campaignfloorbuffer

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{campaignUnderdeliveryMock, platformReportMock}
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.CampaignFloorBufferSchema
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.CampaignFlightData
import com.thetradedesk.plutus.data.transform.campaignfloorbuffer.CampaignFloorBufferCandidateSelectionTransform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.sql.Timestamp
import java.time.LocalDateTime

class CampaignFloorBufferCandidateSelectionTransformTest extends TTDSparkTest {

  val testDate = LocalDateTime.of(2025, 4, 22, 12, 33).toLocalDate

  test("Testing CampaignFloorBufferCandidateSelection generateTodaysFloorBufferDataset") {
    // Test1 -> Latest buffer floor snapshot exists and new candidates generated today
    val yesterdaysFloorBufferDataset = Seq(
      CampaignFloorBufferSchema("abc1", 0.01, testDate.minusDays(3)),
      CampaignFloorBufferSchema("abc2", 0.1, testDate.minusDays(2)),
      CampaignFloorBufferSchema("abc3", 0.01, testDate),
    ).toDS()
    val todaysCandidateCampaignsWithBuffer = Seq(
      CampaignFloorBufferSchema("abc4", 0.01, testDate),
      CampaignFloorBufferSchema("abc2", 0.01, testDate),
    ).toDS()

    val testTodaysFloorBufferDataset1 = generateTodaysFloorBufferDataset(yesterdaysFloorBufferDataset, todaysCandidateCampaignsWithBuffer, Seq.empty[CampaignFloorBufferSchema].toDS())

    assert(testTodaysFloorBufferDataset1.collectAsList().size() == 4, "Validating dataset size")

    // If the campaign exists in today's candidates and latest dataset, today's buffer value will be used.
    val overridenCampaign = testTodaysFloorBufferDataset1.filter($"CampaignId" === "abc2").collect().head
    assert(overridenCampaign.BBF_FloorBuffer == 0.01, "Campaign used today's floor buffer")
    assert(overridenCampaign.AddedDate == testDate, "Campaign date is updated")

    // Check the dataset has correct dates
    val oldCampaign = testTodaysFloorBufferDataset1.filter($"CampaignId" === "abc1").collect().head
    assert(oldCampaign.AddedDate == testDate.minusDays(3), "Campaign has correct Added date")

    // Test 2 -> Campaigns found in rollback dataset should be removed from today's dataset
    val todaysRollbackEligibleCampaigns = Seq(
      CampaignFloorBufferSchema("abc1", 0.01, testDate),
    ).toDS()
    val testTodaysFloorBufferDataset2 = generateTodaysFloorBufferDataset(yesterdaysFloorBufferDataset, todaysCandidateCampaignsWithBuffer, todaysRollbackEligibleCampaigns)
    assert(testTodaysFloorBufferDataset2.collectAsList().size() == 3, "Validating dataset size after rollback")
    assert(testTodaysFloorBufferDataset2.filter($"CampaignId" === "abc1").isEmpty)
  }

  test("Testing CampaignFloorBufferCandidateSelection getFloorBufferCandidateCampaigns") {
    val onePercentFloorBufferCriteria = OnePercentFloorBufferCriteria(
      avgUnderdeliveryFraction=0.02,
      avgCampaignThrottleMetric=0.8,
      openMarketShare=0.01,
      openPathShare=0.01,
      floorBuffer = 0.01
    )
    val mockPlatformReportData = platformReportMock(privateContractId = null)
    val mockSupplyVendorData = Seq(SupplyVendorData("supplyvendor", OpenPathEnabled = true)).toDS()
    val mockFlightData = Seq(CampaignFlightData("newcampaign1",
      Timestamp.valueOf(LocalDateTime.of(2025, 4, 26, 13, 20)),
      Timestamp.valueOf(LocalDateTime.of(2025, 5, 25, 13, 20)))).toDS()
    val campaignUnderDeliveryData = Seq(CampaignUnderDeliveryData("newcampaign1", 0.02, 0.02, 0.8, 0.01)).toDS()
    val latestRollbackBufferDataset = Seq(CampaignFloorBufferSchema("oldcampaign", 0.6, testDate)).toDS()

    // Test1 -> Campaign is added to todays candidates if meets the criteria
    val todaysTestCampaigns1 = getFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      campaignUnderDeliveryData = campaignUnderDeliveryData,
      latestRollbackBufferData = latestRollbackBufferDataset
    )

    assert(todaysTestCampaigns1.collectAsList().size() == 1)

    // Test2 -> Campaign is NOT added to todays candidates if underdelivery fraction >=0.2 and Throttle > 0.8
    val todaysTestCampaignsUnderDeliveryFractionAndThrottleCriteria = getFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      campaignUnderDeliveryData = Seq(CampaignUnderDeliveryData("newcampaign1", 0.02, 0.02, 0.9, 0.01)).toDS(),
      latestRollbackBufferData = latestRollbackBufferDataset
    )

    assert(todaysTestCampaignsUnderDeliveryFractionAndThrottleCriteria.collectAsList().size() == 0)

    // Test3 -> Campaign is added to todays candidates if the rollback date is > 7 days
    val todaysTestCampaignsRollbackDatePriorTo7Days = getFloorBufferCandidateCampaigns(
      date = testDate,
      onePercentFloorBufferCriteria = onePercentFloorBufferCriteria,
      platformReportData = mockPlatformReportData,
      supplyVendorData = mockSupplyVendorData,
      flightData = mockFlightData,
      campaignUnderDeliveryData = campaignUnderDeliveryData,
      latestRollbackBufferData = Seq(CampaignFloorBufferSchema("newcampaign1", 0.6, testDate.minusDays(7))).toDS()
    )

    assert(todaysTestCampaignsRollbackDatePriorTo7Days.collectAsList().size() == 1)

  }

  test("Testing CampaignFloorBufferCandidateSelection getRollbackCampaigns") {
    val yesterdaysCampaignFloorBufferDataset = Seq(
      CampaignFloorBufferSchema("abc1", 0.01, testDate),
      CampaignFloorBufferSchema("abc2", 0.01, testDate.minusDays(1))
    ).toDS()

    val testOnePercentBufferRollbackCriteria = OnePercentFloorBufferRollbackCriteria(
      rollbackUnderdeliveryFraction=0.05,
      rollbackCampaignThrottleMetric=0.8,
    )

    // Test 1 -> Rollback candidates found
    val todaysOnePercentCampaignToRollBack = getRollbackCampaigns(
      testDate,
      yesterdaysCampaignFloorBufferDataset,
      campaignUnderdeliveryMock(
        campaignId = "abc2",
        date = Timestamp.valueOf(LocalDateTime.of(2025, 4, 22, 0, 0)),
        fraction = 0.06,
        throttle = 0.9
      ),
      testOnePercentBufferRollbackCriteria
    )

    assert(todaysOnePercentCampaignToRollBack.collectAsList().size() == 1)
    val rolledBackCampaign = todaysOnePercentCampaignToRollBack.filter($"CampaignId" === "abc2").collect().head
    assert(rolledBackCampaign.AddedDate == testDate)

    // Test 2 -> Rollback candidates not found
    val todaysCampaignToRollBackNotFound = getRollbackCampaigns(
      testDate,
      yesterdaysCampaignFloorBufferDataset,
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
    val todaysCampaignFloorBufferDataset = Seq(
      CampaignFloorBufferSchema("abc1", 0.2, testDate),
      CampaignFloorBufferSchema("abc2", 0.1, testDate),
      CampaignFloorBufferSchema("abc3", 0.1, testDate.minusDays(1))
    ).toDS()

    val todaysCampaignFloorBufferRollbackDataset = Seq(
      CampaignFloorBufferSchema("xyz1", 0.2, testDate),
    ).toDS()

    val testMetrics = generateBbfFloorBufferMetrics(todaysCampaignFloorBufferDataset, todaysCampaignFloorBufferRollbackDataset)

    assert(testMetrics.floorBufferTotalCount == 3)
    assert(testMetrics.floorBufferRollbackTotalCount == 1)

    val expectedBufferFloorCounts = Map(0.2 -> 1, 0.1 -> 2)
    val resultCounts = testMetrics.countByFloorBuffer.collect().map(row => row.getDouble(0) -> row.getLong(1)).toMap
    assert(resultCounts == expectedBufferFloorCounts, s"Expected $expectedBufferFloorCounts but got $resultCounts")
  }
}
