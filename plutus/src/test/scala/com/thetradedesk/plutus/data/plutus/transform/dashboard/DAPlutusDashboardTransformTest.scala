package com.thetradedesk.plutus.data.plutus.transform.dashboard

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adGroupMock, campaignMock, pcResultsMergedMock, roiGoalTypeMock}
import com.thetradedesk.plutus.data.schema.PcResultsMergedSchema
import com.thetradedesk.plutus.data.transform.dashboard.DAPlutusDashboardDataTransform.getDAPlutusMetrics
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupRecord, CampaignRecord, ROIGoalTypeRecord}

class DAPlutusDashboardTransformTest extends TTDSparkTest {

  test("DA Plutus Dashboard data transform test 1 for schema/column correctness") {

    val pcResultsMergedData = Seq(pcResultsMergedMock()).toDS().as[PcResultsMergedSchema]
    val roiGoalTypeData = Seq(roiGoalTypeMock.copy()).toDS().as[ROIGoalTypeRecord]
    val adGroupData = Seq(adGroupMock.copy()).toDS().as[AdGroupRecord]
    val campaignData = Seq(campaignMock.copy()).toDS().as[CampaignRecord]

    val agg_daplutusmetrics = getDAPlutusMetrics(pcResultsMergedData, roiGoalTypeData, adGroupData, campaignData)
    val results = agg_daplutusmetrics.collectAsList().get(0)

    // Test for AdGroup join
    assert(results.PredictiveClearingEnabled == true, "Validating AdGroup join logic")

    // Test for getting ROIGoalTypeName from ROIGoalTypeId
    assert(results.ROIGoalTypeName == Some("Reach"), "Validating get ROIGoalType logic")

    // Test for Campaign join
    assert(results.IsManagedByTTD == Some(false), "Validating Campaign join logic")

    // Test for ChannelSimple from pcResultsMergedSchema
    assert(results.Channel == "Display", "Validating ChannelSimple logic that MobileInApp results in Display")

    // Test for MarketType
    assert(results.MarketType == "OM", "Validating MarketType logic from DetailedMarketType")

    // Test general spend/bid aggregation
    assert(results.PartnerCostInUSD == 0.1, "Validating sum PartnerCost logic")
    assert(results.FeeAmount == Some(0.000012), "Validating sum FeeAmount logic")
    assert(results.FirstPriceAdjustment == Some(0.73), "Validating sum FPA logic")
    assert(results.FinalBid == 36, "Validating sum FinalBid logic")
    assert(results.ImpressionCount == 1, "Validating count ImpressionCount logic")
    assert(results.BidCount == 1, "Validating count BidCount logic")

    // Test for bidding at floor logic - should bypass groupby because final != floor
    assert(results.bidsAtFloorPlutus == Some(0), "Validating bidAtFloorPlutus logic")

    // Test for available surplus
    assert(results.AvailableSurplus == Some(31.0), "Validating AvailableSurplus logic")
  }

  test("DA Plutus Dashboard data transform test 2 for schema/column correctness") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(fpa = null, pcMode = 0, model = "noPcApplied")).toDS().as[PcResultsMergedSchema]
    val roiGoalTypeData = Seq(roiGoalTypeMock.copy()).toDS().as[ROIGoalTypeRecord]
    val adGroupData = Seq(adGroupMock.copy()).toDS().as[AdGroupRecord]
    val campaignData = Seq(campaignMock.copy()).toDS().as[CampaignRecord]

    val agg_daplutusmetrics2 = getDAPlutusMetrics(pcResultsMergedData, roiGoalTypeData, adGroupData, campaignData)
    val results2 = agg_daplutusmetrics2.collectAsList().get(0)

    // Test for PredictiveClearingEnabled heuristic
    assert(results2.PredictiveClearingEnabled == false, "Validating PredictiveClearingEnabled heuristic logic")
  }
}
