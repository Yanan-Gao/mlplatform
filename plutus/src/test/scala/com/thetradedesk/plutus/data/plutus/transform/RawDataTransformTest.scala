package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.MockData.{bidsImpressionsMock, createMbToWinRow, partnerSupplyVendorDiscrepancyAdj, supplyVendorBidding, supplyVendorDealRecord}
import com.thetradedesk.plutus.data.schema.{Deals, Pda, RawMBtoWinSchema, Svb}
import com.thetradedesk.plutus.data.transform.RawDataTransform
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

class RawDataTransformTest extends TTDSparkTest {

  test("Raw Data Transform test for schema/column correctness") {

    val date = java.time.LocalDate.parse("2021-12-25")

    val bidsImps = Seq(bidsImpressionsMock.copy()).toDS().as[BidsImpressionsSchema]

    val mbw = Seq(createMbToWinRow(BidRequestId = "1", winCPM =1.0d, mb2w = 0.8d)).toDS().as[RawMBtoWinSchema]

    val discrep = (Seq(supplyVendorBidding).toDS().as[Svb],
      Seq(partnerSupplyVendorDiscrepancyAdj).toDS().as[Pda],
    Seq(supplyVendorDealRecord).toDS().as[Deals])

    RawDataTransform.transform(date: LocalDate, Seq("Google"), bidsImps, mbw, discrep, 1)(new PrometheusClient("test", "not real"))
    }
}
