package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.{CampaignFloorBufferSchema, MergedCampaignFloorBufferSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{Campaign, bucketCount, getTestBucketUDF, platformWideBuffer}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform._
import com.thetradedesk.plutus.data.transform.campaignbackoff.MergeCampaignBackoffAdjustments.mergeBackoffDatasets
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdGroupRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.matchers.should.Matchers._

import java.time.LocalDateTime


class HadesCampaignAdjustmentsTransformTest extends TTDSparkTest{
  val tolerance = 0.00101

  test("Validating campaign budget buckets") {
    val campaignId = "pmbcej3"
    val campaigns = Seq(campaignId).toDS()
      .withColumnRenamed("value", "CampaignId")
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))

    val res = campaigns.collectAsList().get(0)
    val testBucket = res.getAs[Short]("TestBucket")
    assert(testBucket == 894, "Validating Budget Bucket Hash.")
  }

  test("Validating plutus adjustment calculations") {
    val campaignId = "pmbcej3"
    val floorBuffer = 0.60
    val underdeliveringCampaigns = Seq(CampaignMetaData(campaignId, CampaignType_NewCampaign, floorBuffer)).toDS()

    val pcResultsMergedData = Seq(
      pcResultsMergedMock(campaignId = Some(campaignId), dealId = "0000", adjustedBidCPMInUSD = 6.8800, discrepancy = 1.1, floorPrice = 1, mu = -0.3071f, sigma =  0.6923f, auctionType = 3, maxBidCpmInBucks = 1.0)
    ).toDS().as[PcResultsMergedSchema]

    val bidData = getAllBidData(spark.emptyDataset[PlutusLogsData], spark.emptyDataset[AdGroupRecord], pcResultsMergedData)

    val campaignBidData = getUnderdeliveringCampaignBidData(
      bidData,
      underdeliveringCampaigns
    )

    val results_campaignBidData = campaignBidData.collectAsList().get(0)
    val gen_plutusPushdown = results_campaignBidData.getAs[Double]("gen_gss_pushdown")
    assert(math.abs(gen_plutusPushdown - 0.2426) <= tolerance)
    val gen_bufferFloor = results_campaignBidData.getAs[Double]("gen_bufferFloor")
    val floor_price = results_campaignBidData.getAs[Double]("FloorPrice")
    val expectedFloorBuffer = (floor_price * (1 - floorBuffer))
    assert(gen_bufferFloor == expectedFloorBuffer)
  }

  def prettyPrintRow(row: Row): String = {
    import org.apache.spark.sql.types.StructType
    val schema: StructType = row.schema
    val values = schema.fields.zipWithIndex.map { case (field, i) =>
      s"${field.name}: ${row.get(i)}"
    }
    values.mkString("{\n  ", ",\n  ", "\n}")
  }

  test("Validating plutus adjustment calculations 2 - Verifying non-negative adjustment at buffer = 0.01") {
    val campaignId = "k9vx0p7"
    val floorBuffer = 0.01
    val underdeliveringCampaigns = Seq(CampaignMetaData(campaignId, CampaignType_NewCampaign, floorBuffer)).toDS()

    val pcResultsMergedData = Seq(
      pcResultsMergedMock(
        campaignId = Some(campaignId),
        dealId = "IXTVPD73808257373558",
        adjustedBidCPMInUSD = 16.7124815578058,
        discrepancy = 1.21,
        floorPrice = 16.5,
        maxBidMultiplierCap = 1.2,
        mu = 1.7356233596801758f,
        sigma =  0.6707255840301514f,
        auctionType = 1,
        maxBidCpmInBucks = 45.0
      )
    ).toDS().as[PcResultsMergedSchema]

    val bidData = getAllBidData(spark.emptyDataset[PlutusLogsData], spark.emptyDataset[AdGroupRecord], pcResultsMergedData)
    println("Raw Bid Data: ")
    println(prettyPrintRow(bidData.collectAsList().get(0)))
    println()


    val campaignBidData = getUnderdeliveringCampaignBidData(
      bidData,
      underdeliveringCampaigns
    )

    val res = campaignBidData.collectAsList().get(0)
    println("Campaign Bid Data: ")
    println(prettyPrintRow(res))

    // This is the older adjustment
    val unCappedAdjustment = ((res.getAs[Double]("gen_effectiveDiscrepancy") - res.getAs[Double]("gen_plutusPushdownAtBufferFloor"))
      / (res.getAs[Double]("gen_effectiveDiscrepancy") - res.getAs[Double]("gen_gss_pushdown")))
    unCappedAdjustment shouldEqual -0.40169672531415085 +- tolerance

    res.getAs[Double]("gen_gss_pushdown") shouldEqual 0.450623166820479 +- tolerance

    res.getAs[Double]("gen_bufferFloor") shouldEqual 16.335 +- tolerance

    // This is the new adjustment
    res.getAs[Double]("gen_PCAdjustment") shouldEqual 0.0 +- tolerance
  }

  test("Testing Problem Campaign filtering") {
    // aggregateCampaignBBFOptOutRate
    val campaignBBFOptOutRate = campaignBBFOptOutRateMock
    val yesterdaysData = spark.emptyDataset[HadesAdjustmentSchemaV2]

    val underdeliveryThreshold = 0.05

    // identifyAndHandleProblemCampaigns
    val (hadesAdjustmentsData, metrics) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData, underdeliveryThreshold)

    // Check non-problem campaign
    val campaignA = hadesAdjustmentsData.filter(_.CampaignId == "campaignA").collect().head
    assert(campaignA.HadesBackoff_PCAdjustment == 1)
    assert(!campaignA.Hades_isProblemCampaign)

    // Check campaign with no underdelivery data
    val campaignB = hadesAdjustmentsData.filter(_.CampaignId == "campaignB").collect().head
    assert(campaignB.HadesBackoff_PCAdjustment == 0.7884867455687953)
    // Even though it has an adjustment, this is not a problem campaign since it doesn't have underdelivery data
    assert(!campaignB.Hades_isProblemCampaign)

    // Check problem campaign
    val campaignC = hadesAdjustmentsData.filter(_.CampaignId == "campaignC").collect().head
    assert(campaignC.HadesBackoff_PCAdjustment == 0.6)
    assert(campaignC.Hades_isProblemCampaign)

    // Checking that metrics are properly aggregated
    assert(metrics.filter(m => m.CampaignType == CampaignType_NewCampaign && m.PacingType == PacingStatus_NoPacingData ).head.Count == 1)
    // This is Campaign A
    assert(metrics.filter(m => m.CampaignType == CampaignType_NoAdjustment).head.Count == 1)
  }

  test("Hades Backoff transform test for schema/column correctness") {
    val campaignId = campaignUnderdeliveryForHadesMock().CampaignId
    val floorBuffer = 0.60

    val throttleMetricDataset = Seq(campaignUnderdeliveryForHadesMock()).toDS()

    val pcResultsMergedData = Seq(pcResultsMergedMock(campaignId = Some(campaignId), dealId = "0000", adjustedBidCPMInUSD = 25.01, discrepancy = 1.1, floorPrice = 25, mu = -4.1280107498168945f, sigma = 1.0384914875030518f, maxBidCpmInBucks = 1.23)).toDS().as[PcResultsMergedSchema]

    val plutusLogsData = Seq(pcResultsLogMock("abcd")).toDS().as[PlutusLogsData]
    val adgroupData = Seq(adGroupMock).toDS()

    val bidData = getAllBidData(plutusLogsData, adgroupData, pcResultsMergedData)

    val underDeliveringCampaigns = Seq(CampaignMetaData(campaignId, CampaignType_NewCampaign, floorBuffer)).toDS()
    val campaignBidData = getUnderdeliveringCampaignBidData(bidData, underDeliveringCampaigns)

    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData, throttleMetricDataset )
    val yesterdaysData = spark.emptyDataset[HadesAdjustmentSchemaV2]

    val optOutRates = campaignBBFOptOutRate.collectAsList()
    assert(optOutRates.size() == 1)

    val underdeliveryThreshold = 0.05

    val (hadesAdjustmentsData, _) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData, underdeliveryThreshold)
    val res = hadesAdjustmentsData.collectAsList()
    assert(res.size() == 1)
  }

  test("Merge Hades Backoff and Campaign Backoff test for schema/column correctness") {

    val campaignAdjustmentsHadesData = DataGenerator.generateCampaignAdjustmentsHadesData
    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData.limit(3)
    val todaysCampaignFloorBufferData = DataGenerator.generateMergedCampaignFloorBufferData
    val campaignBufferAdjustmentsHadesData = DataGenerator.generateCampaignBufferAdjustmentsHadesData

    val finalMergedCampaignAdjustments = mergeBackoffDatasets(
      campaignAdjustmentsData,
      campaignAdjustmentsHadesData,
      todaysCampaignFloorBufferData,
      campaignBufferAdjustmentsHadesData
    )
    val res = finalMergedCampaignAdjustments.select(
      "CampaignId",
      "hd_Hades_isProblemCampaign",
      "hd_HadesBackoff_PCAdjustment",
      "pc_CampaignPCAdjustment",
      "MergedPCAdjustment",
      "CampaignBbfFloorBuffer"
    ).collect()

    // Test for campaign that is not Hades Backoff test campaign but in Campaign Backoff.
    // Campaign backoff adjustment should be final adjustment.
    assert(res.contains(Row("campaign1", null, null, 0.75, 0.75, 0.35)))

    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is a Hades problem campaign.
    // The minimum backoff adjustment should be final adjustment.
    assert(res.contains(Row("campaign2", true, 0.6, 0.75, 0.6, platformWideBuffer)))


    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is not a Hades problem campaign.
    // The Campaign Backoff adjustment should be final adjustment.
    assert(res.contains(Row("campaign3", false, 1.0, 0.75, 0.75, platformWideBuffer)))


    // Test for campaign that is Hades Backoff test campaign and not in Campaign Backoff. This is a Hades problem campaign.
    // The Hades Backoff adjustment should be final adjustment.
    assert(res.contains(Row("jkl789", true, 0.9, null, 0.9, platformWideBuffer)))

    // Test for campaign that is not in the merge of Hades backoff and Campaign backoff but is present in the floor buffer snapshot.
    // This campaign should have the final adjustment of 1 and it's floor buffer should be same as in the floor buffer snapshot.
    assert(res.contains(Row("abc123", null, null, null, 1, 0.01)))
    assert(res.contains(Row("abc234", null, null, null, 1, 0.20)))
  }

  test("test for mergeTodayWithYesterdaysData") {
    val underdeliveryThreshold = 0.05

    val todaysData = Seq(
      campaignStatsHadesMock(campaignId = "campaign1", hadesBackoff_PCAdjustment_Options = Array(1.1, 0.9, 1.0), campaignType = CampaignType_AdjustedCampaign),
      campaignStatsHadesMock(campaignId = "campaign2", hadesBackoff_PCAdjustment_Options = Array(0.5, 0.4, 0.3), campaignType = CampaignType_AdjustedCampaign),
      campaignStatsHadesMock(campaignId = "campaign3", hadesBackoff_PCAdjustment_Options = Array(0.5, 0.4, 0.3))
      // No Campaign 4
    ).toDS()

    val yesterdaysData = Seq(
      campaignAdjustmentsHadesMock(campaignId = "campaign1", hadesPCAdjustmentPrevious = Array(0.4), campaignType_Previous = Array(CampaignType_NewCampaign)),
      campaignAdjustmentsHadesMock(campaignId = "campaign2", hadesPCAdjustmentPrevious = Array(0.4)),
      // No Campaign 3
      campaignAdjustmentsHadesMock(campaignId = "campaign4", hadesPCAdjustmentCurrent = 0.5 /* This will be carried on */, hadesPCAdjustmentPrevious = Array(0.5), hadesProblemCampaign = false)
    ).toDS()

    val res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData, underdeliveryThreshold)
    assert(res.collect().length == 4)

    // Testing if yesterday's pushdown is not discarded if the campaignType was CampaignType_NewCampaignNotInThrottleDataset
    // Currently, we dont want to discard the adjustment immediately and since we're now doing rolling averages, it will eventually get discarded regardless.
    val test1 = res.filter($"CampaignId" === "campaign1").collect().head
    assert(test1.AdjustmentQuantile == 50)
    assert(test1.HadesBackoff_PCAdjustment == 0.75) // = (0.4 + 1.1) / 2
    assert(test1.CampaignType == CampaignType_AdjustedCampaign)
    assert(test1.CampaignType_Previous.contains(CampaignType_NewCampaign))
    assert(test1.HadesBackoff_PCAdjustment_Current == 1.1)
    assert(test1.HadesBackoff_PCAdjustment_Previous.contains(0.4))

    // Testing if yesterday's pushdown is maintained
    val test2 = res.filter($"CampaignId" === "campaign2").collect().head
    assert(test2.HadesBackoff_PCAdjustment == 0.45) // = (0.4 + 0.5) / 2
    assert(test2.HadesBackoff_PCAdjustment_Current == 0.5)
    assert(test2.HadesBackoff_PCAdjustment_Previous.contains(0.4))

    // Testing if todays's pushdown is maintained if yesterdays is missing
    val test3 = res.filter($"CampaignId" === "campaign3").collect().head
    assert(test3.HadesBackoff_PCAdjustment == 0.5)
    assert(test3.HadesBackoff_PCAdjustment_Current == 0.5)

    // This null is fine here. We will handle it on reading the next day
    assert(test3.HadesBackoff_PCAdjustment_Previous == null)

    // Testing if yesterday's pushdown is maintained if todays is missing
    val test4 = res.filter($"CampaignId" === "campaign4").collect().head
    assert(test4.HadesBackoff_PCAdjustment == 0.5)
    assert(test4.HadesBackoff_PCAdjustment_Current == 0.5)
    assert(test4.HadesBackoff_PCAdjustment_Previous.contains(0.5))
  }

  test("Testing GetFilteredCampaigns") {
    val campaignUnderdeliveryData = Seq(campaignUnderdeliveryForHadesMock()).toDS()

    val liveCampaigns = Seq("pmbcej3")
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    val yesterdaysCampaigns = Seq("jkl789")
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    val campaignFloorBuffer = Seq(MergedCampaignFloorBufferSchema("jkl789", 0.6, LocalDateTime.of(2025, 4, 22, 12, 33).toLocalDate,"Automatic")).toDS()

    val filteredCampaigns = getFilteredCampaigns(
      campaignThrottleData = campaignUnderdeliveryData,
      campaignFloorBuffer = campaignFloorBuffer,
      potentiallyNewCampaigns = liveCampaigns,
      adjustedCampaigns = yesterdaysCampaigns,
      0.1,
      Some(0.9)
    )

    assert(filteredCampaigns.count() == 2)

    // check the campaigns have expected buffer values
    val test1 = filteredCampaigns.filter($"CampaignId" === "pmbcej3").collect().head
    assert(test1.BBF_FloorBuffer == platformWideBuffer)
    val test2 = filteredCampaigns.filter($"CampaignId" === "jkl789").collect().head
    assert(test2.BBF_FloorBuffer == 0.6)
  }

  // <editor-fold desc="getAdjustmentQuantile Tests">

  test("getAdjustmentQuantile when no adjustment is needed") {
    val result = getAdjustmentQuantile(
      previousQuantiles = Array(40, 40, 40),
      underdeliveryFraction_Current = Some(0.1), // Underdelivery improves
      underdeliveryFraction_Previous = Array(0.2, 0.2, 0.2).map(Some(_)),
      total_BidCount = 9.0,
      total_BidCount_Previous = Array(9.0, 9.0, 9.0),
      bbf_pmp_BidCount = 5,
      bbf_pmp_BidCount_Previous = Array(5, 5, 5),
      underdeliveryThreshold = 0.05,
      contextSize = 3)
    result should be (40)
  }

  test("getAdjustmentQuantile when adjustment is needed because Underdelivery and OptOut % dropped") {
    val result = getAdjustmentQuantile(
      previousQuantiles = Array(40, 40, 40),
      underdeliveryFraction_Current = Some(0.3), // Increased Underdelivery
      underdeliveryFraction_Previous = Array(0.2, 0.2, 0.2).map(Some(_)),
      total_BidCount = 9.0,
      total_BidCount_Previous = Array(9.0, 9.0, 9.0),
      bbf_pmp_BidCount = 6, // Increased PMP bidcount
      bbf_pmp_BidCount_Previous = Array(5, 5, 5),
      underdeliveryThreshold = 0.05,
      contextSize = 3)
    result should be (35)
  }

  test("getAdjustmentQuantile when adjustment is needed but we're at the minimum") {
    val result = getAdjustmentQuantile(
      previousQuantiles = Array(30, 30, 30),
      underdeliveryFraction_Current = Some(0.3), // Increased Underdelivery
      underdeliveryFraction_Previous = Array(0.2, 0.2, 0.2).map(Some(_)),
      total_BidCount = 9.0,
      total_BidCount_Previous = Array(9.0, 9.0, 9.0),
      bbf_pmp_BidCount = 6, // Increased PMP bidcount
      bbf_pmp_BidCount_Previous = Array(5, 5, 5),
      underdeliveryThreshold = 0.05,
      contextSize = 3)
    result should be (30) // Quantile doesn't decrease because 0.3 is minimum rn
  }


  test("getAdjustmentQuantile when adjustment is needed because nothing changed") {
    val result = getAdjustmentQuantile(
      previousQuantiles = Array(40, 40, 40),
      underdeliveryFraction_Current = Some(0.2), // Nothing changes
      underdeliveryFraction_Previous = Array(0.2, 0.2, 0.2).map(Some(_)),
      total_BidCount = 9.0,
      total_BidCount_Previous = Array(9.0, 9.0, 9.0),
      bbf_pmp_BidCount = 5, // Nothing changes
      bbf_pmp_BidCount_Previous = Array(5, 5, 5),
      underdeliveryThreshold = 0.05,
      contextSize = 3)
    result should be (35)
  }
  //</editor-fold>

  // <editor-fold desc="getFinalAdjustment & isReducingOverTime Tests">
  test("getFinalAdjustment with empty Previous Adjustments Array") {
    val result = getFinalAdjustment(0.8, Array(), Some(0.5), 0.6)
    result should be (0.8 +- tolerance) // Only current adjustment is considered
  }

  test("getFinalAdjustment with underdelivery Fraction Exactly at Threshold") {
    val result = getFinalAdjustment(0.7, Array(0.6, 0.5, 0.4), Some(0.5), 0.5)
    result should be (0.5333 +- tolerance) // (0.5 + 0.4 + 0.7) / 3
  }

  test("getFinalAdjustment with Previous Adjustments Array with One Element") {
    val result = getFinalAdjustment(0.9, Array(0.8), Some(0.6), 0.5)
    result should be (0.85 +- tolerance) // (0.8 + 0.9) / 2
  }

  test("getFinalAdjustment with two previous adjustments resulting in an average less than currentAdjustment") {
    val result = getFinalAdjustment(0.7, Array(0.4, 0.3), None)
    result should be (0.4666 +- tolerance) // Average of (0.4 + 0.3 + 0.7) / 3 is 0.4666, which is the min
  }

  test("getFinalAdjustment with currentAdjustment greater than 1") {
    val result = getFinalAdjustment(1.5, Array(0.8, 0.9), None)
    result should be (1.0 +- tolerance) // We truncate the final adjustment at 1
  }

  test("isReducingOverTime with slight decrease below threshold") {
    val result = isReducingOverTime(Array(1.0, 0.95, 0.9, 0.85, 0.8), 0)
    assert(result) // The slope is slightly below the threshold
  }

  test("isReducingOverTime with mixed values and reduction above threshold") {
    val result = isReducingOverTime(Array(0.6, 0.6, 1.0, 1.0, 1.2), -0.02)
    assert(!result) // Mixed values lead to a slope above the threshold
  }

  test("isReducingOverTime with a sharp decline") {
    val result = isReducingOverTime(Array(10.0, 7.0, 4.0, 2.0, 1.0), 0.02)
    assert(result) // The slope is significantly below the threshold
  }

  test("countTrailingZeros - General case with trailing zeros") {
    val arr = Array(1, 2, 0, 0, 0)
    assert(countTrailingZeros(arr) == 3)
  }

  test("countTrailingZeros - No trailing zeros") {
    val arr = Array(1, 2, 3, 4)
    assert(countTrailingZeros(arr) == 0)
  }

  test("countTrailingZeros - All elements are zero") {
    val arr = Array(0, 0, 0, 0)
    assert(countTrailingZeros(arr) == 4)
  }

  test("countTrailingZeros - Empty array case") {
    val arr = Array[Int]()
    assert(countTrailingZeros(arr) == 0)
  }

  test("countTrailingZeros - Single zero element") {
    val arr = Array(0)
    assert(countTrailingZeros(arr) == 1)
  }

  test("countTrailingZeros - Single non-zero element") {
    val arr = Array(5)
    assert(countTrailingZeros(arr) == 0)
  }

  //</editor-fold>

}
