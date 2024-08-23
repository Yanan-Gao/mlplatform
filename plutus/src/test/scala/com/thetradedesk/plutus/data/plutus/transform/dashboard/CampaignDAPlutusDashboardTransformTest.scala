package com.thetradedesk.plutus.data.plutus.transform.dashboard

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData.{adGroupMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.transform.dashboard.CampaignDAPlutusDashboardDataTransform.getCampaignDAPlutusMetrics
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdGroupRecord

class CampaignDAPlutusDashboardTransformTest extends TTDSparkTest {

  test("Campaign DA Plutus Dashboard data transform test for schema/column correctness") {

    val pcResultsMergedData = Seq(pcResultsMergedMock()).toDS().as[PcResultsMergedDataset]
    val adGroupData = Seq(adGroupMock.copy()).toDS().as[AdGroupRecord]

    val agg_daplutusmetrics = getCampaignDAPlutusMetrics(pcResultsMergedData, adGroupData)
    val results = agg_daplutusmetrics.collectAsList().get(0)

    // Test for AdGroup join & PredictiveClearingEnabled heuristic
    assert(results.PredictiveClearingEnabled == true, "Validating AdGroup join logic")

    // Test for ChannelSimple from pcResultsMergedSchema
    assert(results.Channel == "Display", "Validating ChannelSimple logic that MobileInApp results in Display")

    // Test for max PredictiveClearingEnabled
    assert(results.AggPredictiveClearingEnabled == true, "Validating max PredictiveClearingEnabled logic")

    // Test general spend/bid aggregation
    assert(results.PartnerCostInUSD == 0.1, "Validating sum PartnerCost logic")
    assert(results.FeeAmount == Some(0.000012), "Validating sum FeeAmount logic")
    assert(results.FirstPriceAdjustment == Some(0.73), "Validating sum FPA logic")
    assert(results.FinalBid == 36, "Validating sum FinalBid logic")
    assert(results.ImpressionCount == 1, "Validating count ImpressionCount logic")
    assert(results.BidCount == 1, "Validating count BidCount logic")
  }

  test("Campaign DA Plutus Dashboard data transform test 2 for schema/column correctness") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(fpa = null, pcMode = 0, model = "noPcApplied")).toDS().as[PcResultsMergedDataset]
    val adGroupData = Seq(adGroupMock.copy()).toDS().as[AdGroupRecord]

    val agg_daplutusmetrics = getCampaignDAPlutusMetrics(pcResultsMergedData, adGroupData)
    val results = agg_daplutusmetrics.collectAsList().get(0)

    // Test for PredictiveClearingEnabled heuristic
    assert(results.PredictiveClearingEnabled == false, "Validating PredictiveClearingEnabled heuristic logic")
  }
}
