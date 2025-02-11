package com.thetradedesk.plutus.data.plutus.transform

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.plutus.data.mockdata.MockData._
import com.thetradedesk.plutus.data.schema.{MinimumBidToWinData, PcResultsRawLogs, PlutusLogsData, ProductionAdgroupBudgetData}
import com.thetradedesk.plutus.data.transform.PcResultsGeronimoTransform.joinGeronimoPcResultsLog
import com.thetradedesk.plutus.data.{ChannelType, MarketType}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord, PrivateContractRecord}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

class PcResultsGeronimoTransformTest extends TTDSparkTest {

  test("PcResults + Geronimo Transform test for schema/column correctness") {
    val janusVariantMap = Map("model1" -> "variant1")
    val modelVersionsUsedWithPlutus = Map("plutus" -> 42L)
    val modelVersionsUsedWithoutPlutus = Map("model1" -> 42L)

    val geronimoDataset = Seq(
      bidsImpressionsMock(SupplyVendorPublisherId = Some("1")),
      bidsImpressionsMock(FeeFeatureUsage = null, AliasedSupplyPublisherId = Some(2)),
      bidsImpressionsMock(FeeFeatureUsage = Seq (feeFeatureUsageLogMock), SupplyVendorPublisherId = Some("1"), AliasedSupplyPublisherId = Some(2)),
      bidsImpressionsMock(JanusVariantMap = Some(janusVariantMap)),
      bidsImpressionsMock(ModelVersionsUsed = Some(modelVersionsUsedWithoutPlutus)),
      bidsImpressionsMock(ModelVersionsUsed = Some(modelVersionsUsedWithPlutus)),
    ).toDS().as[BidsImpressionsSchema]
    val pcResultsDataset = Seq(pcResultsLogMock("1"), pcResultsLogMock("a")).toDS().as[PlutusLogsData]
    val mbtwDataset = Seq(mbtwDataMock.copy()).toDS().as[MinimumBidToWinData]
    val privateContractDataSet = Seq(privateContractsMock.copy()).toDS().as[PrivateContractRecord]
    val adFormatDataSet = Seq(adFormatMock.copy()).toDS().as[AdFormatRecord]
    val productionAdgroupBudgetDataset = Seq(productionAdgroupBudgetMock.copy()).toDS()
      .withColumn("date",lit(20240394))
      .withColumn("hour",lit(11))
      .as[ProductionAdgroupBudgetData]

    val (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset) =
      joinGeronimoPcResultsLog(geronimoDataset, pcResultsDataset, mbtwDataset,
        privateContractDataSet, adFormatDataSet, productionAdgroupBudgetDataset)

    assert(mergedDataset.count() == 6, "Output rows")
    assert(pcResultsAbsentDataset.count() == 1, "Absent rows (from PCResultsLog)")
    pcResultsAbsentDataset.printSchema()
    assert(mbtwAbsentDataset.count() == 0, "Absent rows (from MBTW Dataset)")

    val resultList = mergedDataset.collectAsList()
    val res = resultList.get(0)
    assert(res.LossReason == mbtwDataMock.LossReason, "Validating mbtw Join")

    // test for channel
    assert(res.Channel == ChannelType.MobileInApp, "Validating Channel Join")
    assert(res.ChannelSimple == ChannelType.Display, "Validating Channel Simplification")

    // test for Value Pacing Column
    assert(res.IsValuePacing.contains(true), "Validating ProductionAdgroupBudgetData Join")
    assert(res.IsUsingPIDController.contains(false), "Validating ProductionAdgroupBudgetData Join")

    // test for DetailedMarketType
    assert(res.DetailedMarketType == MarketType.PrivateAuctionVariablePrice, "Validating DetailedMarketType Join")

    // Test for AspSvpId
    assert(resultList.get(0).AspSvpId == "svp_1", "Validating that svpid is used if aspid is not present")
    assert(resultList.get(1).AspSvpId == "asp_2", "Validating that aspid is used if it is present")
    assert(resultList.get(2).AspSvpId == "asp_2", "Validating that aspid is used if both aspid and svpid are present")
    assert(resultList.get(3).AspSvpId == null, "Validating null in case none are present")

    // Test for IsUsingJanus
    assert(res.IsUsingJanus == false, "Validating Janus fields")

    // Test for Fee Amount column
    assert(res.FeeAmount.isEmpty, "Validating that an empty feeFeatureUsage list results in a none value")

    // Test for BidCap change columns
    assert(res.UseUncappedBidForPushdown == true, "Validating we use the value from the raw log")
    assert(res.UncappedFirstPriceAdjustment == 1.023, "Validating that Uncapped FPA results in a the raw log value")
    assert(res.UncappedBidPrice == 2.0, "Validating we use the value from the raw log")
    assert(res.SnapbackMaxBid == 3.0, "Validating we use the value from the raw log")
    assert(res.MaxBidMultiplierCap == 1.2, "Validating we use the value from the raw log")

    assert(resultList.get(1).FeeAmount == None, "Validating that null feeFeatureUsage results in a none value\"")
    assert(resultList.get(2).FeeAmount == Some(feeFeatureUsageLogMock.FeeAmount), "Validating that the actual feeFeatureUsage.FeeAmount value is propogated")

    // Test for JanusVariantMap
    val res4 = resultList.get(3)
    assert(res4.IsUsingJanus == true)
    assert(res4.JanusVariantMap.isDefined)
    assert(res4.JanusVariantMap.get.equals(janusVariantMap))

    // Test for PlutusVersionUsed
    val res5 = resultList.get(4)  // without plutus
    val res6 = resultList.get(5)  // with plutus
    assert(res4.PlutusVersionUsed.isDefined == false)
    assert(res5.PlutusVersionUsed.isDefined == false)
    assert(res6.PlutusVersionUsed.isDefined)
    assert(res6.PlutusVersionUsed.get == modelVersionsUsedWithPlutus("plutus"))
  }

  test("PcResultsRawLogSchema -> PlutusLogsData test") {
    val rawDataset = Seq(pcResultsRawLogMock.copy()).toDS().as[PcResultsRawLogs]
    val localDateTime = LocalDateTime.of(2024, 7, 8, 16, 0, 0)
    val outputDataset = PlutusLogsData.transformPcResultsRawLog(rawDataset, localDateTime)

    assert(outputDataset.count() == 1, "Output rows")

    val resultList = outputDataset.collectAsList()
    val res = resultList.get(0)

    // Test for BidCap change columns
    assert(res.UseUncappedBidForPushdown == false, "Validating we use the value from the raw log")
    assert(res.UncappedFirstPriceAdjustment == 2.789, "Validating that Uncapped FPA results in a the raw log value")
  }
}
