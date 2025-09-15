package com.thetradedesk.plutus.data.plutus.transform.virtualmaxbidbackoff

import com.thetradedesk.TestUtils.TTDSparkTest
import com.thetradedesk.common.Constants.DeviceType
import com.thetradedesk.plutus.data.mockdata.MockData.{dealsMock, pcResultsMergedMock}
import com.thetradedesk.plutus.data.schema.{Deals, PcResultsMergedSchema}
import com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff.{AdgroupAggDataDataset, AdgroupDealImpMeanFloorDataset}
import com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff.AdgroupMeanDealImpressionFloorTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{CurrencyExchangeRateDataSet, CurrencyExchangeRateRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.LocalDate

class AdgroupMeanDealImpressionFloorTransformTest extends TTDSparkTest {

  test("AdgroupMeanDealImpressionFloorTransform basic transform test for schema/column correctness") {
    val date = LocalDate.parse("2024-06-25")
    val envForRead = "test"
    val variableSpendThreshold = 0.99
    val ctvSpendThreshold = 0.5

    // Create mock PC Results data with CTV and deal impressions
    val pcResultsData = Seq(
      pcResultsMergedMock(
        dealId = "deal1",
        adgroupId = Some("adgroup1"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 10.0,
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      pcResultsMergedMock(
        dealId = "deal1",
        adgroupId = Some("adgroup1"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 15.0,
        floorPrice = 6.0,
        floorPriceInUSD = Some(6.0),
        feeAmount = Some(0.2),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      pcResultsMergedMock(
        dealId = "deal2",
        adgroupId = Some("adgroup2"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 20.0,
        floorPrice = 8.0,
        floorPriceInUSD = Some(8.0),
        feeAmount = Some(0.3),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      // Non-CTV device (should be filtered out)
      pcResultsMergedMock(
        dealId = "deal3",
        adgroupId = Some("adgroup3"),
        deviceType = DeviceType.Mobile.id,
        isImp = true,
        finalBidPrice = 12.0,
        floorPrice = 4.0,
        floorPriceInUSD = Some(4.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      // Non-deal impression (should be filtered out)
      pcResultsMergedMock(
        dealId = null,
        adgroupId = Some("adgroup4"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 8.0,
        floorPrice = 3.0,
        floorPriceInUSD = Some(3.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      )
    ).toDS().as[PcResultsMergedSchema]

    // Create mock deals data
    val dealsData = Seq(
      Deals(
        SupplyVendorId = "vendor1",
        SupplyVendorDealCode = "deal1",
        IsVariablePrice = true,
        FloorPriceCPMInContractCurrency = 4.0
      ),
      Deals(
        SupplyVendorId = "vendor2",
        SupplyVendorDealCode = "deal2",
        IsVariablePrice = true,
        FloorPriceCPMInContractCurrency = 7.0
      )
    ).toDS().as[Deals]

    // Create mock currency exchange rate data
    val currencyExchangeData = Seq(
      (1, "USD", java.math.BigDecimal.valueOf(1.0), java.math.BigDecimal.valueOf(1.0), java.sql.Timestamp.valueOf("2024-06-25 00:00:00")) // USD to USD
    ).toDF("CurrencyCodeId", "CurrencyCode", "fromUSD", "toUSD", "AsOfDateUTC")
      .as[CurrencyExchangeRateRecord]

    // Use the actual aggregation function from the transform
    val aggAdgroupData = AdgroupMeanDealImpressionFloorTransform.aggregateAdgroupData(
      pcResultsData, 
      dealsData, 
      currencyExchangeData, 
      variableSpendThreshold, 
      ctvSpendThreshold
    )

    val result = aggAdgroupData
      .select($"AdGroupId", $"MeanDealImpressionFloor")
      .filter($"IsCTVOnly" && $"MeanDealImpressionFloor".isNotNull)
      .as[AdgroupDealImpMeanFloorDataset]

    // Validate results
    val results = result.collect()
    
    // Should have 2 results (adgroup1 and adgroup2) - adgroup3 is non-CTV, adgroup4 has no deal
    results.length shouldBe 2
    
    // Validate adgroup1 results
    val adgroup1Result = results.find(_.AdgroupId == "adgroup1").get
    adgroup1Result.MeanDealImpressionFloor shouldBe 4.0 // (4.0 + 4.0) / 2 (min of floor prices)
    
    // Validate adgroup2 results  
    val adgroup2Result = results.find(_.AdgroupId == "adgroup2").get
    adgroup2Result.MeanDealImpressionFloor shouldBe 7.0 // 7.0 / 1
  }

  test("AdgroupMeanDealImpressionFloorTransform test CTV filtering and threshold logic") {
    val date = LocalDate.parse("2024-06-25")
    val envForRead = "test"
    val variableSpendThreshold = 0.99
    val ctvSpendThreshold = 0.5

    // Create test data with different CTV spend percentages
    val pcResultsData = Seq(
      // High CTV spend (should pass filter) - 100% CTV
      pcResultsMergedMock(
        dealId = "deal1",
        adgroupId = Some("highCTV"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 100.0, // High spend
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      // Low CTV spend (should be filtered out) - 50% CTV, 50% Mobile
      pcResultsMergedMock(
        dealId = "deal2",
        adgroupId = Some("lowCTV"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 10.0, // CTV spend
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      // Add Mobile device for lowCTV to make it 50% CTV
      pcResultsMergedMock(
        dealId = "deal2",
        adgroupId = Some("lowCTV"),
        deviceType = DeviceType.Mobile.id,
        isImp = true,
        finalBidPrice = 10.0, // Non-CTV spend
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      )
    ).toDS().as[PcResultsMergedSchema]

    val dealsData = Seq(
      Deals(
        SupplyVendorId = "vendor1",
        SupplyVendorDealCode = "deal1",
        IsVariablePrice = true,
        FloorPriceCPMInContractCurrency = 4.0
      ),
      Deals(
        SupplyVendorId = "vendor2",
        SupplyVendorDealCode = "deal2",
        IsVariablePrice = true,
        FloorPriceCPMInContractCurrency = 4.0
      )
    ).toDS().as[Deals]

    val currencyExchangeData = Seq(
      (1, "USD", java.math.BigDecimal.valueOf(1.0), java.math.BigDecimal.valueOf(1.0), java.sql.Timestamp.valueOf("2024-06-25 00:00:00")) // USD to USD
    ).toDF("CurrencyCodeId", "CurrencyCode", "fromUSD", "toUSD", "AsOfDateUTC")
      .as[CurrencyExchangeRateRecord]

    // Use the actual aggregation function from the transform
    val aggAdgroupData = AdgroupMeanDealImpressionFloorTransform.aggregateAdgroupData(
      pcResultsData, 
      dealsData, 
      currencyExchangeData, 
      variableSpendThreshold, 
      ctvSpendThreshold
    )

    val result = aggAdgroupData
      .select($"AdGroupId", $"MeanDealImpressionFloor", $"CTVSpendPercentage", $"IsCTVOnly")
      .filter($"IsCTVOnly" && $"MeanDealImpressionFloor".isNotNull)
      .as[AdgroupDealImpMeanFloorDataset]

    val results = result.collect()
    
    // Should only have highCTV (100% CTV spend > 50% threshold)
    results.length shouldBe 1
    results(0).AdgroupId shouldBe "highCTV"
  }

  test("AdgroupMeanDealImpressionFloorTransform test edge cases - null values and division by zero") {
    val date = LocalDate.parse("2024-06-25")
    val envForRead = "test"
    val variableSpendThreshold = 0.99
    val ctvSpendThreshold = 0.5

    // Create test data with edge cases
    val pcResultsData = Seq(
      // Normal case
      pcResultsMergedMock(
        dealId = "deal1",
        adgroupId = Some("normal"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 10.0,
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      ),
      // Case with null MeanDealImpressionFloor (no deal wins)
      pcResultsMergedMock(
        dealId = null, // This will result in no deal wins
        adgroupId = Some("noDealWins"),
        deviceType = DeviceType.ConnectedTV.id,
        isImp = true,
        finalBidPrice = 10.0,
        floorPrice = 5.0,
        floorPriceInUSD = Some(5.0),
        feeAmount = Some(0.1),
        detailedMarketType = "Private Auction Variable Price",
        isValuePacing = Some(true),
        pcMode = 3,
        currencyCodeId = Some("1")
      )
    ).toDS().as[PcResultsMergedSchema]

    val dealsData = Seq(
      Deals(
        SupplyVendorId = "vendor1",
        SupplyVendorDealCode = "deal1",
        IsVariablePrice = true,
        FloorPriceCPMInContractCurrency = 4.0
      )
    ).toDS().as[Deals]

    val currencyExchangeData = Seq(
      (1, "USD", java.math.BigDecimal.valueOf(1.0), java.math.BigDecimal.valueOf(1.0), java.sql.Timestamp.valueOf("2024-06-25 00:00:00")) // USD to USD
    ).toDF("CurrencyCodeId", "CurrencyCode", "fromUSD", "toUSD", "AsOfDateUTC")
      .as[CurrencyExchangeRateRecord]

    // Use the actual aggregation function from the transform
    val aggAdgroupData = AdgroupMeanDealImpressionFloorTransform.aggregateAdgroupData(
      pcResultsData, 
      dealsData, 
      currencyExchangeData, 
      variableSpendThreshold, 
      ctvSpendThreshold
    )

    val result = aggAdgroupData
      .select($"AdGroupId", $"MeanDealImpressionFloor")
      .filter($"IsCTVOnly" && $"MeanDealImpressionFloor".isNotNull)
      .as[AdgroupDealImpMeanFloorDataset]

    val results = result.collect()
    
    // Should only have the normal case (noDealWins should be filtered out due to null MeanDealImpressionFloor)
    results.length shouldBe 1
    results(0).AdgroupId shouldBe "normal"
  }
}