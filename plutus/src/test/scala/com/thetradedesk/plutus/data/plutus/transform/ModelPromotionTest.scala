package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.MockData.{adFormatMock, bidsImpressionsMock, privateContractsMock}
import com.thetradedesk.plutus.data.transform.ModelPromotionTransform.getDetailedStats
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord, PrivateContractRecord}
import org.scalactic.TolerantNumerics

class ModelPromotionTest extends TTDSparkTest {

  test("Raw Data Transform test for schema/column correctness") {
    val bidsImpressionsData = Seq(bidsImpressionsMock()).toDS().as[BidsImpressionsSchema]
    val privateContractsData = Seq(privateContractsMock.copy()).toDS().as[PrivateContractRecord]
    val adFormatData = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord]

    val stats = getDetailedStats(bidsImpressionsData, privateContractsData, adFormatData)
    val detailedStats = stats.collect.apply(0)

    //    ---- How things are calculated: ----
    //    SubmittedBidAmountInUSD = 10.0
    //    AdjustedBidCPMInUSD = SubmittedBidAmountInUSD * 1000 = 10000.0
    //    BidsFirstPriceAdjustment = 0.9
    //    DiscrepancyAdjustmentMultiplier = 1.0
    //    ImpressionsFirstPriceAdjustment = DiscrepancyAdjustmentMultiplier * BidsFirstPriceAdjustment
    //                                    = 0.9
    //    FloorPriceInUSD = 5000.0
    //    MediaCostCPMInUSD =~ SubmittedBidAmountInUSD * ImpressionsFirstPriceAdjustment * 1000
    //                      = 9000

    implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

    assert(detailedStats.totalWins == 1, "totalWins")
    assert(detailedStats.totalBids == 1, "totalBids")
    // TotalSavings = SubmittedBidAmountInUSD * (1 - BidsFirstPriceAdjustment)
    assert(detailedStats.totalSavings === 1.0, "totalSavings")
    // TotalSpend = MediaCostCPMInUSD / 1000
    assert(detailedStats.totalSpend === 9.0, "totalSpend")
    // Same as savings for impressions
    assert(detailedStats.totalAdjustments === 1.0, "totalAdjustments")
    // totalAvailable = (MediaCostCPMInUSD - FloorPriceInUSD) / 1000)
    assert(detailedStats.totalAvailable === 4.0, "totalAvailable")
  }
}
