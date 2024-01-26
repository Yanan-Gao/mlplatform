package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.MockData.{bidsImpressionsMock, mbtwDataMock, pcResultsLogMock, pcResultsRawLogMock}
import com.thetradedesk.plutus.data.schema.{MinimumBidToWinData, PcResultsRawLogSchema, PlutusLogsData}
import com.thetradedesk.plutus.data.transform.PcResultsGeronimoTransform.joinGeronimoPcResultsLog
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

class PcResultsGeronimoTransformTest extends TTDSparkTest {

  test("PcResults + Geronimo Transform test for schema/column correctness") {
    val geronimoDataset = Seq(bidsImpressionsMock.copy()).toDS().as[BidsImpressionsSchema]
    val pcResultsDataset = Seq(pcResultsLogMock.copy()).toDS().as[PlutusLogsData]
    val mbtwDataset = Seq(mbtwDataMock.copy()).toDS().as[MinimumBidToWinData]

    val joinCols = Seq("BidRequestId")
    val (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset) = joinGeronimoPcResultsLog(geronimoDataset, pcResultsDataset, mbtwDataset, joinCols)

    assert(mergedDataset.count() == 1, "Output rows")
    assert(pcResultsAbsentDataset.count() == 0, "Absent rows (from PCResultsLog)")
    assert(mbtwAbsentDataset.count() == 0, "Absent rows (from MBTW Dataset)")
    assert(mergedDataset.collectAsList().get(0).SupplyVendorLossReason == mbtwDataMock.SupplyVendorLossReason, "Validating mbtw Join")
  }

  test("PcResultsRawLogSchema -> PlutusLogsData test") {
    val pcResultsDataset = Seq(pcResultsRawLogMock.copy()).toDS().as[PcResultsRawLogSchema]
      .select(
        "PlutusLog.*",
        "PredictiveClearingStrategy.*",
        "*"
      ).drop(
        "PlutusLog",
        "PredictiveClearingStrategy"
      ).as[PlutusLogsData]

    assert(pcResultsDataset.count() == 1, "Output rows")
  }
}
