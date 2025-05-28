package com.thetradedesk.plutus.data.plutus.transform.campaignbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.campaignbackoff._
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
}
