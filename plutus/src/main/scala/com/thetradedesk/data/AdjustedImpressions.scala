package com.thetradedesk.data

import java.time.LocalDate

import com.thetradedesk.data.schema.{BidFeedbackDataset, EmpiricalDiscrepancy, AdjImpressions, Impressions}
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions , DataFrameExtensions}
import io.prometheus.client.Gauge
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, broadcast, coalesce, col, concat_ws, lit, round}
import org.apache.spark.sql.types.DoubleType

object AdjustedImpressions {

  // tentative output schema
  // case class
  def getAdjustedImpressions(date: LocalDate, svName: String, svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame, impressionsGauge: Gauge)(implicit spark: SparkSession) = {
    import spark.implicits._
    val impressions = getParquetData[Impressions](date=date, s3path=BidFeedbackDataset.BFS3)
      .filter(col("SupplyVendor") === svName)
      .filter(col("AuctionType") === "FirstPrice")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))

    //TODO: this could move to discrepancy object

    // Empirical Discrepancy from Impressions
    val empDisDf = impressions.alias("bf")
      .select(
        col("PartnerId"),
        col("SupplyVendor"),
        col("DealId"),
        col("AdFormat"),
        coalesce(col("DiscrepancyAdjustmentMultiplier"), lit(1)).alias("DiscrepancyAdjustmentMultiplier")
      )
      .groupBy("PartnerId", "SupplyVendor", "DealId", "AdFormat")
      .agg(round(avg("DiscrepancyAdjustmentMultiplier"), 2).as("EmpiricalDiscrepancy"))


    val impsDf = impressions.alias("bf")
      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pdaDf).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svbDf).alias("svb"), Seq("SupplyVendor"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .withColumn("imp_adjuster",
        coalesce(
          'FirstPriceAdjustment / 'DiscrepancyAdjustmentMultiplier,
          lit(1)/coalesce(
            'DiscrepancyAdjustmentMultiplier,
            'EmpiricalDiscrepancy,
            'pdaDiscrpancyAdjustment,
            'svbDiscrpancyAdjustment,
            lit(1)
          )
        )
      )
      .withColumn("RealMediaCostInUSD", 'MediaCostCPMInUSD / 'DiscrepancyAdjustmentMultiplier)
      .withColumn("RealMediaCost", round('RealMediaCostInUSD, 3))
      .withColumn("i_RealBidPriceInUSD", 'SubmittedBidAmountInUSD * 1000 * 'imp_adjuster)
      .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, 3))

      // only select variable priced deals
      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .select(
        col("BidRequestId"),
        col("DealId").alias("i_DealId"),

        col("MediaCostCPMInUSD").cast(DoubleType).alias("MediaCostCPMInUSD"),
        col("RealMediaCostInUSD").cast(DoubleType).alias("RealMediaCostInUSD"),
        col("RealMediaCost").cast(DoubleType).alias("RealMediaCost"),
        col("DiscrepancyAdjustmentMultiplier").cast(DoubleType).alias("DiscrepancyAdjustmentMultiplier"),

        col("i_RealBidPrice"),
        (col("SubmittedBidAmountInUSD") * 1000).cast(DoubleType).alias("i_OrigninalBidPrice"),
        col("FirstPriceAdjustment").cast(DoubleType).alias("i_FirstPriceAdjustment"),
        col("imp_adjuster")
      )

    impressionsGauge.set(impsDf.count())

    (impsDf.selectAs[AdjImpressions], empDisDf.selectAs[EmpiricalDiscrepancy])

  }
}
