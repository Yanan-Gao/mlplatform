package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.Campaign
import com.thetradedesk.plutus.data.schema.{PcResultsMergedSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignBufferAdjustmentsTransform._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._


class HadesCampaignBufferAdjustmentsTransformTest extends TTDSparkTest{
  val tolerance = 0.00101

  test("Hades Buffer Backoff transform test for schema/column correctness") {
    val campaignId = "0kx8ep0"
    val floorBuffer = 0.1

    val throttleMetricDataset = Seq(campaignUnderdeliveryForHadesMock(campaignId)).toDS()

    val pcResultsMergedData = Seq(pcResultsMergedMock(adgroupId = Some(adGroupMock.AdGroupId), campaignId = Some(campaignId), dealId = "0000", adjustedBidCPMInUSD = 25.01, discrepancy = 1.1, floorPrice = 25, mu = -4.1280107498168945f, sigma = 1.0384914875030518f, maxBidCpmInBucks = 1.23)).toDS().as[PcResultsMergedSchema]

    val plutusLogsData = Seq(pcResultsLogMock("abcd")).toDS().as[PlutusLogsData]
    val adgroupData = Seq(adGroupMockData(campaignId = campaignId)).toDS()

    val bidData = getAllBidData(plutusLogsData, adgroupData, pcResultsMergedData)
    val bidDataList = bidData.collectAsList()
    assert(bidDataList.size() == 1)

    val underDeliveringCampaigns = Seq(CampaignMetaData(campaignId, CampaignType_NewCampaign, floorBuffer, 1.0)).toDS()

    val campaignBidData = getUnderdeliveringCampaignBidData(bidData, underDeliveringCampaigns)
    val campaignBidDataList = campaignBidData.collectAsList()
    assert(campaignBidDataList.size() == 1)

    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData, throttleMetricDataset)
    val yesterdaysData = spark.emptyDataset[HadesBufferAdjustmentSchema]

    val optOutRates = campaignBBFOptOutRate.collectAsList()
    assert(optOutRates.size() == 1)

    val underdeliveryThreshold = 0.05

    val (hadesAdjustmentsData, _) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData, underdeliveryThreshold)
    val res = hadesAdjustmentsData.collectAsList()
    assert(res.size() == 1)
  }

  test("Testing GetFilteredCampaigns") {
    val campaign1 = "newCampaignNoUnderdelivery"
    val campaign2 = "oldCampaign"

    // Campaign4 and Campaign5 are identical except Campaign5 is DOOH
    val campaign4 = "newCampaign"
    val campaign5 = "newCampaignDooh"

    val campaignUnderdeliveryData = Seq(
      campaignUnderdeliveryForHadesMock(campaignId = campaign2),
      campaignUnderdeliveryForHadesMock(campaignId = campaign4),
      campaignUnderdeliveryForHadesMock(campaignId = campaign5, doohSpendPct = 70.0)
    ).toDS()

    val liveCampaigns = Seq(campaign1)
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    val yesterdaysCampaigns = Seq(campaign2)
      .toDF()
      .withColumnRenamed("value", "CampaignId")
      .as[Campaign]

    var campaignAdjustments = Seq(
      PlutusCampaignAdjustment(campaign2, 0.6)
    ).toDS()

    val filteredCampaigns = getFilteredCampaigns(
      campaignThrottleData = campaignUnderdeliveryData,
      campaignAdjustmentsPacing = campaignAdjustments,
      potentiallyNewCampaigns = liveCampaigns,
      adjustedCampaigns = yesterdaysCampaigns,
      0.1
    )

    // Both campaign3 and campaign5 should be excluded.
    assert(filteredCampaigns.count() == 3)

    // check the campaigns have expected buffer values
    val test1 = filteredCampaigns.filter($"CampaignId" === campaign1).collect().head
    assert(test1.CampaignType == CampaignType_NewCampaign)
    val test2 = filteredCampaigns.filter($"CampaignId" === campaign2).collect().head
    assert(test2.CampaignType == CampaignType_AdjustedCampaign)
    val test3 = filteredCampaigns.filter($"CampaignId" === campaign4).collect().head
    assert(test3.CampaignType == CampaignType_NewCampaign)
  }
}
