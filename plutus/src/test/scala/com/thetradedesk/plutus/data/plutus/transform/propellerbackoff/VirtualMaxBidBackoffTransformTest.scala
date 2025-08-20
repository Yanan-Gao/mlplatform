package com.thetradedesk.plutus.data.plutus.transform.propellerbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff.VirtualMaxBidBackoffSchema
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff.VirtualMaxBidBackoffTransform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions.{col, count, expr, least, lit, sum, when}

class VirtualMaxBidBackoffTransformTest extends TTDSparkTest {
  test("VirtualMaxBidBackoff transform test (simple) for schema/column correctness") {
    val campaignId1 = "Camp123"
    val yesterdaysData = Seq(virtualMaxBidBackoffMock(campaignId1)).toDS()

    val campaignThrottleDataset = Seq(
      campaignUnderdeliveryForHadesMock(),
      campaignUnderdeliveryForHadesMock(campaignId = campaignId1)
    ).toDS()

    val relevantCampaigns = getRelevantCampaigns(yesterdaysData, campaignThrottleDataset)
    println(s"relevantCampaigns Results: ")
    relevantCampaigns.show(relevantCampaigns.count().toInt, truncate = false)

    val adGroupData = Seq(adGroupMock, adGroupMockData("adgroup123", campaignId1)).toDS()
    val pcbidDataset = Seq(pcResultsMergedMock(campaignId = Some(adGroupMock.CampaignId), dealId = "0000", adjustedBidCPMInUSD = 25.01, discrepancy = 1.1, floorPrice = 25, mu = -4.1280107498168945f, sigma = 1.0384914875030518f, maxBidCpmInBucks = 1.23, uncappedBidPrice = 2000)).toDS().as[PcResultsMergedSchema]
    val optoutDataset = Seq(pcResultsLogMock("abcd", adGroupId = "adgroup123")).toDS().as[PlutusLogsData]

    val relevantBidData = getRelevantBidData(adGroupData, optoutDataset, pcbidDataset)
    println(s"getRelevantBidData Results: ")
    relevantBidData.show()

    val backoffResult = getVirtualMaxBidBackoffValues(relevantBidData, relevantCampaigns)
    println(s"getVirtualMaxBidBackoffValues Results: ")
    backoffResult.show()

    val metrics = getMetrics(backoffResult)
    assert(metrics.length == 2)

    for (metric <- metrics) {
      println(s"CampaignType: ${metric.CampaignType}, " +
        s"PacingType: ${metric.PacingType}, " +
        s"Multiplier: ${metric.VirtualMaxBid_Multiplier}, " +
        s"Quantile: ${metric.VirtualMaxBid_Quantile}, " +
        s"Count: ${metric.Count}")
    }
  }


  test("getLastChanges should return correct daysSinceLastChange and indexOfLastChange") {
    val testCases = Seq(
      // Case 1: No change at all
      (Array(5, 5, 5, 5), (4, -1)),
      // Case 2: Change occurred recently
      (Array(9, 8, 7, 6), (1, 2)),
      // Case 3: Change occurred long ago
      (Array(6, 5, 5, 5), (3, 0)),
      // Case 4: Change in the middle
      (Array(2, 3, 2, 2), (2, 1)),
      // Case 5: Multiple changes, last one is lowest
      (Array(5, 6, 7, 4), (1, 2)),
      // Case 6: Only one element
      (Array(10), (1, -1))
    )

    for ((input, expected) <- testCases) {
      val result = getLastChanges(input)
      assert(result === expected, s"For input ${input.mkString(",")} expected $expected but got $result")
    }
  }


  test("Testing getAdjustmentFromQuantile") {
    val options = Seq(10.0, 20.0, 30.0, 40.0, 50.0)

    val expectedResults = Map(
      90 -> 50.0,
      80 -> 45.0,
      70 -> 40.0,
      60 -> 35.0,
      50 -> 30.0,
      40 -> 25.0,
      30 -> 20.0,
      20 -> 15.0,
      10 -> 10.0
    )

    for ((quantile, expected) <- expectedResults) {
      assert(getAdjustmentFromQuantile(quantile, options) === expected, s"Failed for quantile $quantile")
    }

    assertThrows[IllegalArgumentException] {
      getAdjustmentFromQuantile(0, options)
    }
  }

  test("Testing GetPropellerQuantile") {
    // Not enough history → return default
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.1,
      underdeliveryFraction_History = null,
      virtualMaxBidQuantileHistory = null
    ) === VirtualMaxBidCap_Quantile_Default)
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.1,
      underdeliveryFraction_History = Array(0.1),
      virtualMaxBidQuantileHistory = Array(90)
    ) === VirtualMaxBidCap_Quantile_Default)

    // Recent change with underdelivery → return pre-adjustment quantile
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.4,
      underdeliveryFraction_History = Array(0.3, 0.3, 0.3),
      virtualMaxBidQuantileHistory = Array(90, 90, 90, 80, 80)
    ) === 90)

    // Recent change with no underdelivery → keep current quantile
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.01,
      underdeliveryFraction_History = Array(0.1, 0.1, 0.1, 0.01),
      virtualMaxBidQuantileHistory = Array(90, 90, 90, 80, 80)
    ) === 80)

    // Stable delivery over waiting period → lower quantile
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.01,
      underdeliveryFraction_History = Array(0.01, 0.01, 0.01, 0.01, 0.01),
      virtualMaxBidQuantileHistory = Array(90, 90, 90, 90, 90)
    ) === 80)

    // Unstable delivery over waiting period → keep current quantile
    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.4,
      underdeliveryFraction_History = Array(0.3, 0.3, 0.3, 0.01, 0.3),
      virtualMaxBidQuantileHistory = Array(90, 90, 90, 90, 90)
    ) === 90)

    assert(getVirtualMaxBidQuantile(
      underdeliveryFraction = 0.25,
      underdeliveryFraction_History = Array(0.3, 0.2, 0.25),
      virtualMaxBidQuantileHistory = Array(80, 82, 85)
    ) === 85)
  }
}
