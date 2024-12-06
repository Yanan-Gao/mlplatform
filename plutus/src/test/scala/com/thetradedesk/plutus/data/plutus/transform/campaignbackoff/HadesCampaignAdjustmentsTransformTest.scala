package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.DataGenerator
import com.thetradedesk.plutus.data.mockdata.MockData.{campaignBBFOptOutRateMock, campaignUnderdeliveryForHadesMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignThrottleMetricSchema}
import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignAdjustmentsTransform.mergeCampaignBackoffWithHadesCampaignBackoff
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform.{getCampaignBidData, identifyAndHandleProblemCampaigns}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Row


class HadesCampaignAdjustmentsTransformTest extends TTDSparkTest{
  test("Hades Backoff transform test for schema/column correctness") {

    val pcResultsMergedData_BudgetBucketTest = Seq(pcResultsMergedMock(campaignId = Some("pmbcej3"))).toDS().as[PcResultsMergedDataset]

    val campaignBidData_BudgetBucketTest = getCampaignBidData(pcResultsMergedData_BudgetBucketTest, testSplit = Some(0.9))
    val results_campaignBidData_BudgetBucketTest = campaignBidData_BudgetBucketTest.collectAsList().get(0)
    val testBucket = results_campaignBidData_BudgetBucketTest.getAs[Short]("TestBucket")
    assert(testBucket == 894, "Validating Budget Bucket Hash.")


    // getCampaignBidData
    val pcResultsMergedData = Seq(pcResultsMergedMock(dealId = "0000", adjustedBidCPMInUSD = 25.01, discrepancy = 1.1, floorPrice = 25, mu = -4.1280107498168945f, sigma = 1.0384914875030518f)).toDS().as[PcResultsMergedDataset]

    val campaignBidData = getCampaignBidData(pcResultsMergedData, testSplit = Some(0.9))
    val results_campaignBidData = campaignBidData.collectAsList().get(0)
    val gen_PCAdjustment = results_campaignBidData.getAs[Double]("gen_PCAdjustment")
    assert(gen_PCAdjustment == 0.7884867455687953, "Validating Hades Adjustment Factor Calculation")


    // aggregateCampaignBBFOptOutRate
    val campaignBBFOptOutRate = campaignBBFOptOutRateMock.select("*")


    // identifyAndHandleProblemCampaigns
    val campaignUnderdeliveryData = Seq(campaignUnderdeliveryForHadesMock.copy()).toDS().as[CampaignThrottleMetricSchema]
    val hadesAdjustmentsData = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, campaignUnderdeliveryData, campaignBidData, underdeliveryThreshold = 0.1)

    // Check problem campaign
    val results_hadesAdjustmentsData_1 = hadesAdjustmentsData.collectAsList().get(1)
    val HadesBackoff_PCAdjustment_1: Option[Double] = results_hadesAdjustmentsData_1.HadesBackoff_PCAdjustment
    val Hades_isProblemCampaign_1: Boolean = results_hadesAdjustmentsData_1.Hades_isProblemCampaign
    assert(HadesBackoff_PCAdjustment_1 == Some(0.7884867455687953) && Hades_isProblemCampaign_1 == true, "Validating Identifying Problem Campaigns: when is a problem campaign.")
    // Check non-problem campaign
    val results_hadesAdjustmentsData_0 = hadesAdjustmentsData.collectAsList().get(0)
    val HadesBackoff_PCAdjustment_0: Option[Double] = results_hadesAdjustmentsData_0.HadesBackoff_PCAdjustment
    val Hades_isProblemCampaign_0: Boolean = results_hadesAdjustmentsData_0.Hades_isProblemCampaign
    assert(HadesBackoff_PCAdjustment_0 == None && Hades_isProblemCampaign_0 == false, "Validating Identifying Problem Campaigns: when not a problem campaign.")


    // identifyAndHandleProblemCampaigns - test when no underdelivery data
    val emptyCampaignUnderdeliveryData = spark.emptyDataset[CampaignThrottleMetricSchema]
    val hadesAdjustmentsDataV2 = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, emptyCampaignUnderdeliveryData, campaignBidData, underdeliveryThreshold = 0.1)

    // Check problem campaign
    val results_hadesAdjustmentsDataV2_1 = hadesAdjustmentsDataV2.collectAsList().get(1)
    val HadesBackoff_PCAdjustmentV2_1: Option[Double] = results_hadesAdjustmentsDataV2_1.HadesBackoff_PCAdjustment
    val Hades_isProblemCampaignV2_1: Boolean = results_hadesAdjustmentsDataV2_1.Hades_isProblemCampaign
    assert(HadesBackoff_PCAdjustmentV2_1 == Some(0.7884867455687953) && Hades_isProblemCampaignV2_1 == true, "Validating Identifying Problem Campaigns when no CampaignUnderdeliveryData and is problem campaign.")
    val results_hadesAdjustmentsDataV2_0 = hadesAdjustmentsDataV2.collectAsList().get(0)
    val HadesBackoff_PCAdjustmentV2_0: Option[Double] = results_hadesAdjustmentsDataV2_0.HadesBackoff_PCAdjustment
    val Hades_isProblemCampaignV2_0: Boolean = results_hadesAdjustmentsDataV2_0.Hades_isProblemCampaign
    assert(HadesBackoff_PCAdjustmentV2_0 == None && Hades_isProblemCampaignV2_0 == false, "Validating Identifying Problem Campaigns when no CampaignUnderdeliveryData and is not problem campaign.")

  }

  test("Merge Hades Backoff and Campaign Backoff test for schema/column correctness") {

    val campaignAdjustmentsHadesData = DataGenerator.generateCampaignAdjustmentsHadesData
    val campaignAdjustmentsData = DataGenerator.generateCampaignAdjustmentsPacingData.limit(3)
    val finalMergedCampaignAdjustments = mergeCampaignBackoffWithHadesCampaignBackoff(campaignAdjustmentsData, campaignAdjustmentsHadesData)
    val results_finalMergedCampaignAdjustments = finalMergedCampaignAdjustments.collectAsList()
    //    finalMergedCampaignAdjustments.printSchema()
    //    finalMergedCampaignAdjustments.show(truncate = false)

    // Test for campaign that is not Hades Backoff test campaign but in Campaign Backoff.
    // Campaign backoff adjustment should be final adjustment.
    val res_fromCampaignBackoff = results_finalMergedCampaignAdjustments.get(0)
    val Hades_isProblemCampaign_fromCampaignBackoff = res_fromCampaignBackoff.fieldIndex("Hades_isProblemCampaign")
    val isTest_fromCampaignBackoff = res_fromCampaignBackoff.getAs[Boolean]("IsTest")
    val merged_CampaignPCAdjustment_fromCampaignBackoff = res_fromCampaignBackoff.getAs[Double]("CampaignPCAdjustment")
    val CampaignBackoff_PCAdjustment_fromCampaignBackoff = res_fromCampaignBackoff.getAs[Double]("CampaignBackoff_PCAdjustment")
    val HadesBackoff_PCAdjustment_fromCampaignBackoff = res_fromCampaignBackoff.fieldIndex("HadesBackoff_PCAdjustment")
    assert(
      res_fromCampaignBackoff.isNullAt(Hades_isProblemCampaign_fromCampaignBackoff) &&
        isTest_fromCampaignBackoff == false &&
        merged_CampaignPCAdjustment_fromCampaignBackoff == 0.75 &&
        CampaignBackoff_PCAdjustment_fromCampaignBackoff == 0.75 &&
        res_fromCampaignBackoff.isNullAt(HadesBackoff_PCAdjustment_fromCampaignBackoff),
      "Validating final merged dataset: Only Campaign Backoff campaigns in final merged dataset")


    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is a Hades problem campaign.
    // The minimum backoff adjustment should be final adjustment.
    val res_fromHadesBackoff_flagged = results_finalMergedCampaignAdjustments.get(1)
    val Hades_isProblemCampaign_fromHadesBackoff_flagged = res_fromHadesBackoff_flagged.getAs[Boolean]("Hades_isProblemCampaign")
    val isTest_fromHadesBackoff_flagged = res_fromHadesBackoff_flagged.getAs[Boolean]("IsTest")
    val merged_CampaignPCAdjustment_fromHadesBackoff_flagged = res_fromHadesBackoff_flagged.getAs[Double]("CampaignPCAdjustment")
    val CampaignBackoff_PCAdjustment_fromHadesBackoff_flagged = res_fromHadesBackoff_flagged.getAs[Double]("CampaignBackoff_PCAdjustment")
    val HadesBackoff_PCAdjustment_fromHadesBackoff_flagged = res_fromHadesBackoff_flagged.getAs[Double]("HadesBackoff_PCAdjustment")
    assert(
      Hades_isProblemCampaign_fromHadesBackoff_flagged == true &&
        isTest_fromHadesBackoff_flagged == true &&
        merged_CampaignPCAdjustment_fromHadesBackoff_flagged == 0.6 &&
        CampaignBackoff_PCAdjustment_fromHadesBackoff_flagged == 0.75 &&
        HadesBackoff_PCAdjustment_fromHadesBackoff_flagged == 0.6,
      "Validating final merged dataset: Hades Backoff campaigns that are in Test and flagged as problem.")

    // Test for campaign that is Hades Backoff test campaign and in Campaign Backoff. This is not a Hades problem campaign.
    // The Campaign Backoff adjustment should be final adjustment.
    val res_fromHadesBackoff_notFlagged = results_finalMergedCampaignAdjustments.get(2)
    val Hades_isProblemCampaign_fromHadesBackoff_notFlagged = res_fromHadesBackoff_notFlagged.getAs[Boolean]("Hades_isProblemCampaign")
    val isTest_fromHadesBackoff_notFlagged = res_fromHadesBackoff_notFlagged.getAs[Boolean]("IsTest")
    val merged_CampaignPCAdjustment_fromHadesBackoff_notFlagged = res_fromHadesBackoff_notFlagged.getAs[Double]("CampaignPCAdjustment")
    val CampaignBackoff_PCAdjustment_fromHadesBackoff_notFlagged = res_fromHadesBackoff_notFlagged.getAs[Double]("CampaignBackoff_PCAdjustment")
    val HadesBackoff_PCAdjustment_fromHadesBackoff_notFlagged = res_fromHadesBackoff_notFlagged.fieldIndex("HadesBackoff_PCAdjustment")
    assert(
      Hades_isProblemCampaign_fromHadesBackoff_notFlagged == false &&
        isTest_fromHadesBackoff_notFlagged == true &&
        merged_CampaignPCAdjustment_fromHadesBackoff_notFlagged == 0.75 &&
        CampaignBackoff_PCAdjustment_fromHadesBackoff_notFlagged == 0.75 &&
        res_fromHadesBackoff_notFlagged.isNullAt(HadesBackoff_PCAdjustment_fromHadesBackoff_notFlagged),
      "Validating final merged dataset: Hades Backoff campaigns that are in Test but not flagged as problem.")

    // Test for campaign that is Hades Backoff test campaign and not in Campaign Backoff. This is a Hades problem campaign.
    // The Hades Backoff adjustment should be final adjustment.
    val res_fromHadesBackoff_outer = results_finalMergedCampaignAdjustments.get(3)
    val Hades_isProblemCampaign_fromHadesBackoff_outer = res_fromHadesBackoff_outer.getAs[Boolean]("Hades_isProblemCampaign")
    val isTest_fromHadesBackoff_outer = res_fromHadesBackoff_outer.getAs[Boolean]("IsTest")
    val merged_CampaignPCAdjustment_fromHadesBackoff_outer = res_fromHadesBackoff_outer.getAs[Double]("CampaignPCAdjustment")
    val CampaignBackoff_PCAdjustment_fromHadesBackoff_outer = res_fromHadesBackoff_outer.fieldIndex("CampaignBackoff_PCAdjustment")
    val HadesBackoff_PCAdjustment_fromHadesBackoff_outer = res_fromHadesBackoff_outer.getAs[Double]("HadesBackoff_PCAdjustment")
    assert(
      Hades_isProblemCampaign_fromHadesBackoff_outer == true &&
        isTest_fromHadesBackoff_outer == true &&
        merged_CampaignPCAdjustment_fromHadesBackoff_outer == 0.9 &&
        res_fromHadesBackoff_outer.isNullAt(CampaignBackoff_PCAdjustment_fromHadesBackoff_outer) &&
        HadesBackoff_PCAdjustment_fromHadesBackoff_outer == 0.9,
      "Validating final merged dataset: Hades Backoff campaigns that are in Test but not in Campaign Backoff Pacing dataset.")
  }

}
