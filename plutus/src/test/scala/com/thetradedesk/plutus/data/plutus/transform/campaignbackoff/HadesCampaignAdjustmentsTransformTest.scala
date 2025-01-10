package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PlutusLogsData}
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignAdjustmentsTransform.mergeBackoffDatasets
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdGroupRecord
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._


class HadesCampaignAdjustmentsTransformTest extends TTDSparkTest{
  val tolerance = 0.001

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
    val underdeliveringCampaigns = Seq(FilteredCampaignData(campaignId, CampaignType_NewCampaignNotPacing)).toDS()

    val pcResultsMergedData = Seq(
      pcResultsMergedMock(campaignId = Some(campaignId), dealId = "0000", adjustedBidCPMInUSD = 6.8800, discrepancy = 1.1, floorPrice = 1, mu = -0.3071f, sigma =  0.6923f, auctionType = 3)
    ).toDS().as[PcResultsMergedDataset]

    val bidData = getAllBidData(spark.emptyDataset[PlutusLogsData], spark.emptyDataset[AdGroupRecord], pcResultsMergedData)

    val campaignBidData = getUnderdeliveringCampaignBidData(
      bidData,
      underdeliveringCampaigns
    )

    val results_campaignBidData = campaignBidData.collectAsList().get(0)
    val gen_plutusPushdown = results_campaignBidData.getAs[Double]("gen_gss_pushdown")
    assert(math.abs(gen_plutusPushdown - 0.2426) <= tolerance)
  }

  test("Testing Problem Campaign filtering") {
    // aggregateCampaignBBFOptOutRate
    val campaignBBFOptOutRate = campaignBBFOptOutRateMock.select("*")
    val yesterdaysData = spark.emptyDataset[CampaignAdjustmentsHadesSchema]

    // identifyAndHandleProblemCampaigns
    val (hadesAdjustmentsData, metrics) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData)

    // Check non-problem campaign
    val nonProblemCampaign = hadesAdjustmentsData.collectAsList().get(0)
    assert(nonProblemCampaign.HadesBackoff_PCAdjustment == 1)
    assert(!nonProblemCampaign.Hades_isProblemCampaign)

    // Check problem campaign
    val problemCampaign = hadesAdjustmentsData.collectAsList().get(1)
    assert(problemCampaign.HadesBackoff_PCAdjustment == 0.7884867455687953)
    assert(problemCampaign.Hades_isProblemCampaign)

    // Checking that metrics are properly aggregated
    assert(metrics.filter(_._1==CampaignType_NewCampaignNotInThrottleDataset).head._2 == 1)
    assert(metrics.filter(_._1==CampaignType_NewCampaignNotPacing).head._2 == 1)
  }

  test("Hades Backoff transform test for schema/column correctness") {
    val campaignId = "abcd"
    val pcResultsMergedData = Seq(pcResultsMergedMock(campaignId = Some(campaignId), dealId = "0000", adjustedBidCPMInUSD = 25.01, discrepancy = 1.1, floorPrice = 25, mu = -4.1280107498168945f, sigma = 1.0384914875030518f)).toDS().as[PcResultsMergedDataset]

    val plutusLogsData = Seq(pcResultsLogMock("abcd")).toDS().as[PlutusLogsData]
    val adgroupData = Seq(adGroupMock).toDS()

    val bidData = getAllBidData(plutusLogsData, adgroupData, pcResultsMergedData)

    val underDeliveringCampaigns = Seq(FilteredCampaignData(campaignId, CampaignType_NewCampaignNotPacing)).toDS()
    val campaignBidData = getUnderdeliveringCampaignBidData(bidData, underDeliveringCampaigns)

    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData)
    val yesterdaysData = spark.emptyDataset[CampaignAdjustmentsHadesSchema]

    val optOutRates = campaignBBFOptOutRate.collectAsList()
    assert(optOutRates.size() == 1)

    val (hadesAdjustmentsData, metrics) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData)
    val res = hadesAdjustmentsData.collectAsList()
    assert(res.size() == 1)

    assert(metrics.filter(_._1==CampaignType_NewCampaignNotPacing).head._2 == 1)
  }


  test("Merge Hades Backoff and Campaign Backoff test for schema/column correctness") {

    val campaignAdjustmentsHadesData = DataGenerator.generateCampaignAdjustmentsHadesData
    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData.limit(3)
    val finalMergedCampaignAdjustments = mergeBackoffDatasets(campaignAdjustmentsData, campaignAdjustmentsHadesData)
    val res = finalMergedCampaignAdjustments.collect()


    // Test for campaign that is not Hades Backoff test campaign but in Campaign Backoff.
    // Campaign backoff adjustment should be final adjustment.
    val justCampaignBackoff = res.filter(_.CampaignId == "campaign1").head
    assert(justCampaignBackoff.Hades_isProblemCampaign.isEmpty)
    assert(justCampaignBackoff.HadesBackoff_PCAdjustment.isEmpty)
    assert(justCampaignBackoff.CampaignPCAdjustment.contains(0.75))
    assert(justCampaignBackoff.MergedPCAdjustment == 0.75)


    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is a Hades problem campaign.
    // The minimum backoff adjustment should be final adjustment.
    val bothHadesAndCampaignBackoff = res.filter(_.CampaignId == "campaign2").head
    assert(bothHadesAndCampaignBackoff.Hades_isProblemCampaign.contains(true))
    assert(bothHadesAndCampaignBackoff.HadesBackoff_PCAdjustment.contains(0.6))
    assert(bothHadesAndCampaignBackoff.CampaignPCAdjustment.contains(0.75))
    assert(bothHadesAndCampaignBackoff.MergedPCAdjustment == 0.6)


    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is not a Hades problem campaign.
    // The Campaign Backoff adjustment should be final adjustment.
    val notHadesProblemCampaign = res.filter(_.CampaignId == "campaign3").head
    assert(notHadesProblemCampaign.Hades_isProblemCampaign.contains(false))
    assert(notHadesProblemCampaign.HadesBackoff_PCAdjustment.contains(1.0))
    assert(notHadesProblemCampaign.CampaignPCAdjustment.contains(0.75))
    assert(notHadesProblemCampaign.MergedPCAdjustment == 0.75)


    // Test for campaign that is Hades Backoff test campaign and not in Campaign Backoff. This is a Hades problem campaign.
    // The Hades Backoff adjustment should be final adjustment.
    val hadesProblemCampaign = res.filter(_.CampaignId == "jkl789").head
    assert(hadesProblemCampaign.Hades_isProblemCampaign.contains(true))
    assert(hadesProblemCampaign.HadesBackoff_PCAdjustment.contains(0.9))
    assert(hadesProblemCampaign.CampaignPCAdjustment.isEmpty)
    assert(hadesProblemCampaign.MergedPCAdjustment == 0.9)
  }


  test("Test that reading the merged data back into campaign adjustments works") {
    val campaignAdjustmentsHadesData = DataGenerator.generateCampaignAdjustmentsHadesData
    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData.limit(3)
    val finalMergedCampaignAdjustments = mergeBackoffDatasets(campaignAdjustmentsData, campaignAdjustmentsHadesData)
    val campaignAdjustmentsPacingData = finalMergedCampaignAdjustments.filter($"CampaignPCAdjustment".isNotNull).selectAs[CampaignAdjustmentsPacingSchema]

    assert(campaignAdjustmentsPacingData.collect().length == 3)

  }

  test("test for mergeTodayWithYesterdaysData") {
    // In these test, we drop `HadesBackoff_PCAdjustment_Old`
    // because that shouldn't exist in the dataframe going into mergeTodayWithYesterdaysData


    // Testing if yesterday's pushdown is discarded if the campaignType was CampaignType_NewCampaignNoUnderdelivery
    var todaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustmentCurrent = Some(0.8), campaignType = CampaignType_AdjustedCampaignPacing)
      .toDF()
    var yesterdaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustment = 0.4, campaignType = CampaignType_NewCampaignNotInThrottleDataset)
    var res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData).collect().head

    assert(res.CampaignId == "campaign3")
    assert(res.HadesBackoff_PCAdjustment == 0.8)
    assert(res.CampaignType == CampaignType_AdjustedCampaignPacing)
    assert(res.CampaignType_Yesterday.contains(CampaignType_NewCampaignNotInThrottleDataset))
    assert(res.HadesBackoff_PCAdjustment_Current.contains(0.8))
    assert(res.HadesBackoff_PCAdjustment_Old.contains(0.4))


    // Testing if yesterday's pushdown is maintained
    todaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustmentCurrent = Some(1.0))
      .toDF()
    yesterdaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustment = 0.4)
    res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData).collect().head

    assert(res.CampaignId == "campaign3")
    assert(res.HadesBackoff_PCAdjustment == 0.4)
    assert(res.HadesBackoff_PCAdjustment_Current.contains(1.0))
    assert(res.HadesBackoff_PCAdjustment_Old.contains(0.4))

    // Testing if todays's pushdown is maintained if yesterdays is missing
    todaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustmentCurrent = Some(0.5))
      .toDF()
    yesterdaysData = spark.emptyDataset[CampaignAdjustmentsHadesSchema]
    res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData).collect().head

    assert(res.CampaignId == "campaign3")
    assert(res.HadesBackoff_PCAdjustment == 0.5)
    assert(res.HadesBackoff_PCAdjustment_Current.contains(0.5))
    assert(res.HadesBackoff_PCAdjustment_Old.isEmpty)

    // Testing if yesterday's pushdown is maintained if todays is missing
    todaysData = spark.emptyDataset[CampaignAdjustmentsHadesSchema]
      .toDF()
    yesterdaysData = campaignAdjustmentsHadesMock(campaignId = "campaign3", hadesPCAdjustment = 0.5, hadesProblemCampaign = false)
    res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData).collect().head

    assert(res.CampaignId == "campaign3")
    assert(res.HadesBackoff_PCAdjustment == 0.5)
    assert(res.HadesBackoff_PCAdjustment_Current.isEmpty)
    assert(res.HadesBackoff_PCAdjustment_Old.contains(0.5))
  }

  test("Testing splitting merged dataset") {
    val campaignAdjustmentsMergedData = spark.emptyDataset[CampaignAdjustmentsMergedDataset]
    campaignAdjustmentsMergedData.filter($"CampaignPCAdjustment".isNotNull).selectAs[CampaignAdjustmentsPacingSchema].collect()
    campaignAdjustmentsMergedData
      .filter($"HadesBackoff_PCAdjustment".isNotNull && $"HadesBackoff_PCAdjustment" < 1.0)
      .as[CampaignAdjustmentsHadesSchema].collect()

    // These two will throw an error if there is an issue with the schema
  }

  test("Testing GetFilteredCampaigns") {
    val campaignUnderdeliveryData = Seq(campaignUnderdeliveryForHadesMock).toDS()

    val liveCampaigns = Seq("pmbcej3")
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    val yesterdaysCampaigns = Seq("jkl789")
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    val filteredCampaigns = getFilteredCampaigns(
      campaignUnderdeliveryData = campaignUnderdeliveryData,
      potentiallyNewCampaigns = liveCampaigns,
      yesterdaysCampaigns = yesterdaysCampaigns,
      0.1,
      Some(0.9)
    )

    assert(filteredCampaigns.count() == 2)
  }

}
