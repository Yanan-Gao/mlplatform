package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.MockData.{bidsImpressionsMock, createMbToWinRow, partnerSupplyVendorDiscrepancyAdj, supplyVendorBidding, supplyVendorDealRecord}
import com.thetradedesk.plutus.data.schema.{Deals, Pda, RawLostBidData, Svb}
import com.thetradedesk.plutus.data.transform.RawDataTransform
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

class RawDataTransformTest extends TTDSparkTest {

  test("Raw Data Transform test for schema/column correctness") {

    val date = java.time.LocalDate.parse("2021-12-25")

    val bidsImps = Seq(bidsImpressionsMock()).toDS().as[BidsImpressionsSchema]

    val rawLostBidData = Seq(createMbToWinRow(BidRequestId = "1", WinCPM =1.0d, mbtw = 0.8d)).toDS().as[RawLostBidData]

    val discrep = (Seq(supplyVendorBidding).toDS().as[Svb],
      Seq(partnerSupplyVendorDiscrepancyAdj).toDS().as[Pda],
    Seq(supplyVendorDealRecord).toDS().as[Deals])

    RawDataTransform.transform(date: LocalDate, Seq("Google"), bidsImps, rawLostBidData, discrep, 1)(new PrometheusClient("test", "not real"))
    }
}
