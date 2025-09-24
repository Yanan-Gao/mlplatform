package com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff

import com.thetradedesk.common.Constants.DeviceType
import com.thetradedesk.plutus.data.loadParquetData
import com.thetradedesk.plutus.data.schema.{Deals, DiscrepancyDataset, PcResultsMergedDataset, PcResultsMergedSchema}
import com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff.{AdgroupAggData, AdgroupAggDataDataset, AdgroupDealImpMeanFloorDataset, AdgroupMeanDealImpressionFloorDataset}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{CurrencyExchangeRateDataSet, CurrencyExchangeRateRecord}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.IntegerType

import java.time.LocalDate

object AdgroupMeanDealImpressionFloorTransform {

  def transform(date: LocalDate,
                envForRead: String,
                variableSpendThreshold: Double,
                ctvSpendThreshold: Double
               ): Dataset[AdgroupDealImpMeanFloorDataset] = {

    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
    val exchangeRateDataSet = CurrencyExchangeRateDataSet().readDate(date)
    val pcResultsGeronimoData = PcResultsMergedDataset.readDate(date, env = envForRead, nullIfColAbsent = true)
    
    val aggAdgroupData = aggregateAdgroupData(pcResultsGeronimoData, deals, exchangeRateDataSet, variableSpendThreshold, ctvSpendThreshold)

    // write the upstream data so we can debug if need be
    AdgroupAggData.writeData(date, aggAdgroupData)

    val adgroupMeanDealImpressionFloor = aggAdgroupData
      .select($"AdgroupID", $"MeanDealImpressionFloor")
      .filter($"IsCtvOnly" && $"MeanDealImpressionFloor".isNotNull)
      .as[AdgroupDealImpMeanFloorDataset]

    AdgroupMeanDealImpressionFloorDataset.writeData(date, adgroupMeanDealImpressionFloor)

    adgroupMeanDealImpressionFloor
  }

  def aggregateAdgroupData(pcResultsData: Dataset[PcResultsMergedSchema],
                          deals: Dataset[Deals],
                          exchangeRateDataSet: Dataset[CurrencyExchangeRateRecord],
                          variableSpendThreshold: Double,
                          ctvSpendThreshold: Double): Dataset[AdgroupAggDataDataset] = {

    pcResultsData
      .filter($"IsValuePacing" && $"PredictiveClearingMode" === lit(3))
      .withColumn("IsDealImp", when($"DealId".isNotNull && $"IsImp", 1).otherwise(0))
      .withColumn("IsImp", $"IsImp".cast(IntegerType))
      .withColumn("VariablePriceSpend", when($"DetailedMarketType" === "Private Auction Variable Price", $"FinalBidPrice" * $"isImp").otherwise(lit(0)))
      .withColumn("CTVSpend", when($"DeviceType" === lit(DeviceType.ConnectedTV.id), $"FinalBidPrice" * $"isImp").otherwise(lit(0)))
      .join(broadcast(deals), $"DealId" === $"SupplyVendorDealCode", "left")
      // if a currency code in the contract list doesnt have an exchange rate we probably have bigger issues, so joining this as Inner
      .join(broadcast(exchangeRateDataSet), Seq("CurrencyCodeId"), "inner")
      .withColumn("ContractFloorInUSD", $"FloorPriceCPMInContractCurrency" / $"fromUSD")
      .withColumn("DealImpressionFloor",
        least($"FloorPriceInUSD", $"ContractFloorInUSD") // The core logic: MIN(SSP Floor, Library Floor)
      )
      .groupBy("AdGroupId")
      .agg(
        sum($"FloorPriceInUSD" * $"IsImp").as("SumImpressionFloor"),
        sum($"DealImpressionFloor" * $"IsDealImp").as("SumDealImpressionFloor"),
        sum($"FinalBidPrice" * $"isImp").as("Spend"),
        count("*").as("Bids"),
        sum(col("IsImp")).as("Wins"),
        sum(col("IsDealImp")).as("DealWins"),
        sum($"VariablePriceSpend").as("VariablePriceSpend"),
        sum($"FeeAmount").as("SumFeeAmount"),
        sum($"InitialBid").as("SumInitialBid"),
        sum("CTVSpend").as("CTVSpend")
      )
      .filter($"SumFeeAmount" > lit(0))
      .withColumn("VariablePriceSpendPercentage", $"VariablePriceSpend" / $"Spend")
      .withColumn("CTVSpendPercentage", $"CTVSpend" / $"Spend")
      .withColumn("MeanImpressionFloor", $"SumImpressionFloor" / $"Wins")
      .withColumn("MeanDealImpressionFloor", $"SumDealImpressionFloor" / $"DealWins")
      .withColumn("IsDealOnly", when($"VariablePriceSpendPercentage" > variableSpendThreshold, true).otherwise(false))
      .withColumn("IsCTVOnly", when($"CTVSpendPercentage" > ctvSpendThreshold, true).otherwise(false))
      .cache()
      .as[AdgroupAggDataDataset]
  }
}
