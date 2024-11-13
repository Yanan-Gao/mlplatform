package com.thetradedesk.plutus.data.plutus.transform.dashboard

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.transform.dashboard.PlutusDashboardDataTransform.{getAggMetrics, getMarginAttribution}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.SupplyVendorRecord

class PlutusDashboardTransformTest extends TTDSparkTest {

  test("Plutus Dashboard data transform test 1 for schema/column correctness - ex with winning imp, open path adjustment, PC pushing down without hitting floor") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(supplyVendor = Some("cafemedia"), feeAmount = Some(0.000012), strategy = 30, mu = -0.9295179843902588f, sigma = 0.9264574646949768f)).toDS().as[PcResultsMergedDataset]
    val supplyVendorData = Seq(supplyVendorMock(supplyVendorName = "cafemedia", openPathEnabled = true)).toDS().as[SupplyVendorRecord]

    // Calculations behind Margin Attribution
    val add_marginattcols = getMarginAttribution(pcResultsMergedData, supplyVendorData)
    val check_calc = add_marginattcols.collectAsList().get(0)

    // Test for checking FinalBidPrice calculation - both proposedBid & calc_FinalBidPrice should be the same
    assert(check_calc.getDouble(check_calc.schema.fieldIndex("calc_FinalBidPrice")) === check_calc.getDouble(check_calc.schema.fieldIndex("proposedBid")), "Validating calculating Final Bid logic")

    // Metric aggregation
    val agg_merged = getAggMetrics(add_marginattcols)
    val results = agg_merged.collectAsList().get(0)
    print(results)

    // Test for Margin Attribution output
    assert(results.FactorCombination == Some("OpenPath-"), "Validating Margin Attribution logic when SSP is OpenPath")

    // Test for hasParams, hasMBTW, hasDeal
    assert(results.hasParams == true, "Validating hasParams logic by checking Mu")
    assert(results.hasMBTW == true, "Validating hasMBTW logic by checking mbtw")

    // Test for ChannelSimple from pcResultsMergedSchema
    assert(results.Channel == "Display", "Validating ChannelSimple logic that MobileInApp results in Display")

    // Test general spend/bid aggregation
    assert(results.PartnerCostInUSD == 0.1, "Validating sum PartnerCost logic")
    assert(results.FeeAmount == Some(0.000012), "Validating sum FeeAmount logic")
    assert(results.ImpressionCount == 1, "Validating count ImpressionCount logic")
    assert(results.BidCount == 1, "Validating count BidCount logic")

    // Test for bidsAtFloor plutus
    assert(results.bidsAtFloorPlutus == Some(0), "Validating bidAtFloorPlutus logic when final bid is above floor")

    // Test for available surplus - when bid is above floor
    assert(results.AvailableSurplus == Some(31.0), "Validating AvailableSurplus logic when final bid is above floor")

    // Test for mbtw analysis when bid wins
    assert(results.spend_cpm == Some(36), "Validating MBTW win logic")
    assert(results.overbid_cpm == Some(16), "Validating MBTW win logic v2")

    assert(results.sum_mode_logNorm == Some(0.16732095609076852), "Validating mode_logNorm calculation")

    assert(results.avg_plutusPushdown == None, "Validating not populating avg_plutusPushdown")
    assert(results.avg_FirstPriceAdjustment == None, "Validating not populating avg_FirstPriceAdjustment")
  }

  test("Plutus Dashboard data transform test 2 for schema/column correctness - ex with losing bid, campaign pc adjustment, Strategy < 100, PC pushing down without hitting floor") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(channel = "Display", isImp = false, feeAmount = Some(0.000012), finalBidPrice = 17, strategy = 75)).toDS().as[PcResultsMergedDataset]
    val supplyVendorData = Seq(supplyVendorMock(supplyVendorName = "casale")).toDS().as[SupplyVendorRecord]

    // Calculations behind Margin Attribution
    val add_marginattcols = getMarginAttribution(pcResultsMergedData, supplyVendorData)
    val check_calc = add_marginattcols.collectAsList().get(0)

    // Test for checking FinalBidPrice calculation - both proposedBid & calc_FinalBidPrice should be the same
    assert(check_calc.getDouble(check_calc.schema.fieldIndex("calc_FinalBidPrice")) === check_calc.getDouble(check_calc.schema.fieldIndex("proposedBid")), "Validating calculating Final Bid logic")


    // Metric aggregation
    val agg_merged = getAggMetrics(add_marginattcols)
    val results = agg_merged.collectAsList().get(0)

    // Test for Margin Attribution output
    assert(results.FactorCombination == Some("CampaignPCAdjustment-"), "Validating Margin Attribution logic when Campaign PC Adjustment is applied")

    // Test for mbtw analysis when bid loses
    assert(results.non_spend_cpm == Some(17), "Validating MBTW loss logic")
    assert(results.underbid_cpm == Some(3), "Validating MBTW loss logic v2")
  }

  test("Plutus Dashboard data transform test 3 for schema/column correctness - ex with winning imp, pressure reducer adjustment (remaining at 100%), BBAO<1, PC pushing down below floor") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(channel = "Display", feeAmount = Some(0.000012), baseBidAutoOpt = 0.9, finalBidPrice = 30.0, floorPrice = 30.0, strategy = 100)).toDS().as[PcResultsMergedDataset]
    val supplyVendorData = Seq(supplyVendorMock()).toDS().as[SupplyVendorRecord]


    // Calculations behind Margin Attribution
    val add_marginattcols = getMarginAttribution(pcResultsMergedData, supplyVendorData)
    val check_calc = add_marginattcols.collectAsList().get(0)

    // Test for checking FinalBidPrice calculation - both proposedBid & calc_FinalBidPrice should be different, and calc_FinalBidPrice should equal FloorPrice
    assert(check_calc.getDouble(check_calc.schema.fieldIndex("proposedBid")) === 27.777777777777775, "Validating calculating Final Bid logic before hitting floor cap logic")
    assert(check_calc.getDouble(check_calc.schema.fieldIndex("calc_FinalBidPrice")) === 30.0, "Validating calculating Final Bid logic when floor cap is hit")


    // Metric aggregation
    val agg_merged = getAggMetrics(add_marginattcols)
    val results = agg_merged.collectAsList().get(0)

    // Test for Margin Attribution output
    assert(results.FactorCombination == Some("Bbao-Floor"), "Validating Margin Attribution logic when BBAO<1 and PC pushes below floor")

    // Test for ChannelSimple from pcResultsMergedSchema
    assert(results.Channel == "Display", "Validating ChannelSimple logic that Display results in Display")

    // Test for MarketType
    assert(results.MarketType == "OM", "Validating MarketType logic from DetailedMarketType")

    // Test for bidsAtFloor plutus - when bid is at floor
    assert(results.bidsAtFloorPlutus == Some(1), "Validating bidAtFloorPlutus logic when final bid is at floor")

    // Test for available surplus - when bid is at floor
    assert(results.AvailableSurplus == Some(0), "Validating AvailableSurplus logic when final bid is at floor")
  }

  test("Plutus Dashboard data transform test 4 for schema/column correctness - ex with lost bid, pressure reducer adjustment, DealId=null, Floor=0, Mu=0") {

    val pcResultsMergedData = Seq(pcResultsMergedMock(dealId = null, pcMode = 0, isImp = false, feeAmount = None, floorPrice = 0, mu = 0.0f)).toDS().as[PcResultsMergedDataset]
    val supplyVendorData = Seq(supplyVendorMock()).toDS().as[SupplyVendorRecord]


    // Calculations behind Margin Attribution
    val add_marginattcols = getMarginAttribution(pcResultsMergedData, supplyVendorData)

    // Metric aggregation
    val agg_merged = getAggMetrics(add_marginattcols)
    val results = agg_merged.collectAsList().get(0)

    // Test for Margin Attribution output
    assert(results.FactorCombination == Some("NoPlutus"), "Validating Margin Attribution logic when PC disabled")

    // Test for hasParams, hasDeal
    assert(results.hasParams == false, "Validating hasParams logic")
    //assert(results.hasMBTW == false, "Validating hasMBTW logic")

    // Test general spend/bid aggregation
    assert(results.FeeAmount == None, "Validating sum FeeAmount logic when ")
    assert(results.ImpressionCount == 0, "Validating count ImpressionCount logic")
    assert(results.BidCount == 1, "Validating count BidCount logic")

  }
}
