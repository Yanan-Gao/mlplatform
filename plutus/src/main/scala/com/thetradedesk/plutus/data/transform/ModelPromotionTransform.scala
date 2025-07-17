package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.plutus.data.transform.SharedTransforms.{AddChannelUsingAdFormat, AddMarketType}
import com.thetradedesk.plutus.data.{IMPLICIT_DATA_SOURCE, loadParquetDataHourly}
import com.thetradedesk.spark.datasets.sources.{AdFormatDataSet, AdFormatRecord, PrivateContractDataSet, PrivateContractRecord}
import com.thetradedesk.spark.util.LocalParquet
import com.thetradedesk.spark.util.TTDConfig.environment
import job.ModelPromotion.spark.implicits._
import job.ModelPromotion.{spark, timestamp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode}

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

case class ModelPromotionStats(PlutusTfModel: Option[String], totalBids: Long, totalWins: Long, totalSavings: Double, totalAdjustments: Double, totalSpend: Double) {
  val savingsRate = totalSavings / (totalSpend + totalSavings)
  val winRate = totalWins.toDouble / totalBids
  val realizedSurplusNorm = totalSavings / totalBids.toDouble
  val potentialSurplusNorm = totalAdjustments / totalBids.toDouble
}

case class ModelPromotionDetailedStats(PlutusTfModel: Option[String],
                                       Channel: Option[String],
                                       SupplyVendor: Option[String],
                                       DetailedMarketType: Option[String],
                                       totalBids: Long,
                                       totalWins: Long,
                                       totalAdjustments: Double,
                                       totalAvailable: BigDecimal,
                                       totalSavings: Double,
                                       totalSpend: Double)


object ModelPromotionTransform {

  val PATH_DETAILED_CSV: String = "detailed_results"

  def Transform(endDateTime: LocalDateTime, lookback: Int, outputPath: String): Array[ModelPromotionStats] = {

    val datapath = if (environment == LocalParquet) {
      "local-data/"
    } else {
      BidsImpressions.BIDSIMPRESSIONSS3
    }

    val startDateTime = endDateTime.minusHours(lookback)

    val bidsImpressionsData = loadParquetDataHourly[BidsImpressionsSchema](
      f"${datapath}prod/bidsimpressions",
      endDateTime,
      source = Some(IMPLICIT_DATA_SOURCE),
      lookBack = Some(lookback)
    ).where($"LogEntryTime" >= Timestamp.from(startDateTime.toInstant(ZoneOffset.UTC))
        and $"LogEntryTime" <= Timestamp.from(endDateTime.toInstant(ZoneOffset.UTC))
        and $"JanusVariantMap".isNull
        and $"PredictiveClearingMode.value" === 3)


    val privateContractsData = PrivateContractDataSet().readDate(endDateTime.toLocalDate)
    val adFormatData = AdFormatDataSet().readDate(endDateTime.toLocalDate)

    // Detailed stats
    val detailedQuery = getDetailedStats(bidsImpressionsData, privateContractsData, adFormatData).cache()

    val detailed_output_path = s"$outputPath/${timestamp}_detailed_result/"

    detailedQuery.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header","true").csv(detailed_output_path)

    val simpleResults = detailedQuery.groupBy("PlutusTfModel")
      .agg(
        sum("totalBids").as("totalBids"),
        sum("totalWins").as("totalWins"),
        sum("totalAdjustments").as("totalAdjustments"),
        sum("totalAvailable").as("totalAvailable"),
        sum("totalSavings").as("totalSavings"),
        sum("totalSpend").as("totalSpend")
      ).as[ModelPromotionStats].collect()

    spark.close()
    simpleResults
  }


  def getDetailedStats(bidsImpressionsData: Dataset[BidsImpressionsSchema], privateContractsData: Dataset[PrivateContractRecord], adFormatData: Dataset[AdFormatRecord]) = {
    var res = bidsImpressionsData
      // Using BidsFirstPriceAdjustment instead of ImpressionsFirstPriceAdjustment
      // ImpressionsFirstPriceAdjustment =~ BidsFirstPriceAdjustment * DiscrepancyAdjustmentMultiplier
      .withColumn("savings",
        when($"isImp" === false, 0.0)
          .otherwise($"SubmittedBidAmountInUSD" * (lit(1.0) - $"BidsFirstPriceAdjustment"))
      )
      // We use AdjustedBidCPMInUSD here because SubmittedBidAmountInUSD is null when the bid is not an impression
      // Adjustments is pushdown, but for all bids (not just the impressions).
      .withColumn("adjustments", $"AdjustedBidCPMInUSD" / 1000 * (lit(1.0) - $"BidsFirstPriceAdjustment"))
      // Available pushdown tells us how much more pushdown is available for the model
      .withColumn("available", ($"MediaCostCPMInUSD" - $"FloorPriceInUSD") / 1000)
      // MediaCostCPMInUSD =~ AdjustedBidCPMInUSD * ImpressionsFirstPriceAdjustment
      // (or MediaCostCPM =~ SubmittedBidAmountInUSD * ImpressionsFirstPriceAdjustment * 1000)
      .withColumn("spend",
        when($"isImp" === false, 0.0)
          .otherwise($"MediaCostCPMInUSD" / 1000)
      )
      .withColumn("AdFormat", concat($"AdWidthInPixels", lit("x"), $"AdHeightInPixels"))
      .groupBy("PlutusTfModel", "AuctionType", "DealId", "PrivateContractId", "RenderingContext", "AdFormat", "DeviceType", "SupplyVendor")
      .agg(
        count("*").as("totalBids"),
        sum(col("isimp").cast("long")).as("totalWins"),
        sum("adjustments").as("totalAdjustments"),
        sum("available").as("totalAvailable"),
        sum("savings").as("totalSavings"),
        sum("spend").as("totalSpend"),
      )

    res = AddChannelUsingAdFormat(res, adFormatData)
    res = AddMarketType(res, privateContractsData)
    res.groupBy("PlutusTfModel", "Channel", "SupplyVendor", "DetailedMarketType")
      .agg(
        sum("totalBids").as("totalBids"),
        sum("totalWins").as("totalWins"),
        sum("totalAdjustments").as("totalAdjustments"),
        sum("totalAvailable").as("totalAvailable"),
        sum("totalSavings").as("totalSavings"),
        sum("totalSpend").as("totalSpend")
      ).as[ModelPromotionDetailedStats]
  }

}