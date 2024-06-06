package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.{ChannelType, MarketType}
import com.thetradedesk.plutus.data.MockData._
import com.thetradedesk.plutus.data.schema.{MinimumBidToWinData, PcResultsRawLogs, PlutusLogsData, ProductionAdgroupBudgetData}
import com.thetradedesk.plutus.data.transform.PcResultsGeronimoTransform.joinGeronimoPcResultsLog
import com.thetradedesk.plutus.data.transform.PlutusDataTransform.transformPcResultsRawLog
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord, PrivateContractRecord}
import com.thetradedesk.streaming.records.rtb.FeeFeatureUsageLogBackingData

class PcResultsGeronimoTransformTest extends TTDSparkTest {

  test("PcResults + Geronimo Transform test for schema/column correctness") {
    val janusVariantMap = Map("model1" -> "variant1")

    val geronimoDataset = Seq (
      bidsImpressionsMock(),
      bidsImpressionsMock(null),
      bidsImpressionsMock(Seq {feeFeatureUsageLogMock}),
      bidsImpressionsMock(JanusVariantMap = Some(janusVariantMap))
    ).toDS().as[BidsImpressionsSchema]
    val pcResultsDataset = Seq(pcResultsLogMock.copy()).toDS().as[PlutusLogsData]
    val mbtwDataset = Seq(mbtwDataMock.copy()).toDS().as[MinimumBidToWinData]
    val privateContractDataSet = Seq(privateContractsMock.copy()).toDS().as[PrivateContractRecord]
    val adFormatDataSet = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord]
    val productionAdgroupBudgetDataset = Seq(productionAdgroupBudgetMock.copy()).toDS().as[ProductionAdgroupBudgetData]

    val (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset) =
      joinGeronimoPcResultsLog(geronimoDataset, pcResultsDataset, mbtwDataset,
        privateContractDataSet, adFormatDataSet, productionAdgroupBudgetDataset)

    assert(mergedDataset.count() == 4, "Output rows")
    assert(pcResultsAbsentDataset.count() == 0, "Absent rows (from PCResultsLog)")
    assert(mbtwAbsentDataset.count() == 0, "Absent rows (from MBTW Dataset)")

    val resultList = mergedDataset.collectAsList()
    val res = resultList.get(0)
    assert(res.LossReason == mbtwDataMock.LossReason, "Validating mbtw Join")

    // test for channel
    assert(res.Channel == ChannelType.MobileInApp, "Validating Channel Join")
    assert(res.ChannelSimple == ChannelType.Display, "Validating Channel Simplification")

    // test for Value Pacing Column
    assert(res.IsValuePacing == Some(true), "Validating ProductionAdgroupBudgetData Join")
    assert(res.IsUsingPIDController == Some(false), "Validating ProductionAdgroupBudgetData Join")

    // test for DetailedMarketType
    assert(res.DetailedMarketType == MarketType.PrivateAuctionVariablePrice, "Validating DetailedMarketType Join")

    // Test for AspSvpId
    assert(res.AspSvpId == "asp_1", "Validating DetailedMarketType Join")

    // Test for IsUsingJanus
    assert(res.IsUsingJanus == false, "Validating Janus fields")

    // Test for Fee Amount column
    assert(res.FeeAmount == None, "Validating that an empty feeFeatureUsage list results in a none value")

    assert(resultList.get(1).FeeAmount == None, "Validating that null feeFeatureUsage results in a none value\"")
    assert(resultList.get(2).FeeAmount == Some(feeFeatureUsageLogMock.FeeAmount), "Validating that the actual feeFeatureUsage.FeeAmount value is propogated")

    val res4 = resultList.get(3)
    assert(res4.IsUsingJanus == true)
    assert(res4.JanusVariantMap.isDefined)
    assert(res4.JanusVariantMap.get.equals(janusVariantMap))
  }

  test("PcResultsRawLogSchema -> PlutusLogsData test") {
    val rawDataset = Seq(pcResultsRawLogMock.copy()).toDS().as[PcResultsRawLogs]
    val outputDataset = transformPcResultsRawLog(rawDataset)

    assert(outputDataset.count() == 1, "Output rows")
  }
}
