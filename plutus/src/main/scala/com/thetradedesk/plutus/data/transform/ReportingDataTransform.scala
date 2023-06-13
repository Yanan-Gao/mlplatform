/*
package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.shared.schemas.{BidFeedbackDataset, BidFeedbackRecord, BidRequestDataset, BidRequestRecord}
import com.thetradedesk.plutus.data.{explicitDatePart, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate

object ReportingDataTransform {

  val S3BASEOUT = "s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/"

  def transform(date: LocalDate, model: String, testIdStringMatch: String, sspList: Seq[String])(implicit prometheus: PrometheusClient): Unit = {

    val avgCpm = prometheus.createGauge("average_cpm", "average cpm per model", labelNames = "model")
    val savingsPerImp = prometheus.createGauge("savings_per_imp", "savings per impression per model", labelNames = "model")
    val pctStarvingAg = prometheus.createGauge("pct_starving_adgoups", "percentage of adgroups starving per type", labelNames = "model")
    val winRate = prometheus.createGauge("win_rate", "win rate per model", labelNames = "model")
    val totalRows = prometheus.createGauge("plutus_reporting_rows", "total rows of bid used in plutus reporting")
    val agLogHist = prometheus.createHistogram("AgSavingsPctDiffSavings", "adgroup percentage difference savings", -3.0D, 0.5D, 14)

    val dfFull = getFullData(date, sspList)

    // cache to mem/write to s3
    totalRows.set(dfFull.cache().count())
    dfFull.write.mode(SaveMode.Overwrite).parquet(S3BASEOUT + "performancedata/fulldata/" + explicitDatePart(date))
    val verticaQueryDat = oldVerticaQuery(dfFull)
    verticaQueryDat.write.mode(SaveMode.Overwrite).parquet(S3BASEOUT + "performancedata/verticaQuery/" + explicitDatePart(date))

    val models = verticaQueryDat.select($"model_simple").collect.map(_.getString(0))

    for (model <- models) {
      val modelDat = verticaQueryDat.filter($"model_simple" === model)
      avgCpm.labels(model).set(modelDat.select($"avg_bid_price").head.getDouble(0))
      savingsPerImp.labels(model).set(modelDat.select($"avg_savings").head.getDouble(0))
      winRate.labels(model).set(modelDat.select($"wr").head.getDouble(0))
    }

    val agDat = adgroupLevel(dfFull).select($"log_points").filter($"log_points".isNotNull).collect.map(_.getDouble(0))

    agDat.foreach(i => agLogHist.observe(i))

  }

  def getFullData(date: LocalDate, sspList: Seq[String]) = {
    val dfImps = loadParquetData[BidFeedbackRecord](s3path = BidFeedbackDataset.BFS3, date = date)
    .select(
      $"BidRequestId",
      $"SupplyVendor",
      $"MediaCostCPMInUSD",
      $"DiscrepancyAdjustmentMultiplier",
      $"FeedbackBidderCacheMachineName" as "BidderCacheMachineName",
      $"SubmittedBidAmountInUSD",
      ($"SubmittedBidAmountInUSD" * 1000).alias("OrigninalBidPrice"),
      (($"SubmittedBidAmountInUSD" * 1000) * (lit(1.0) / $"DiscrepancyAdjustmentMultiplier")).alias("DiscrepancyMediaCost")
    )
      .filter(
        $"SupplyVendor".isin(sspList: _*) &&
          ($"BidderCacheMachineName".like("ca2%") || $"BidderCacheMachineName".like("de1%"))
      ).withColumn(
      "RealMediaCostInUSD", $"MediaCostCPMInUSD" / $"DiscrepancyAdjustmentMultiplier")
    .drop("BidderCacheMachineName")
      .drop("SupplyVendor")

    val dfBids = loadParquetData[BidRequestRecord](s3path = BidRequestDataset.BIDSS3, date = date)
      .select(
      $"BidRequestId",
      $"DeviceType.value".alias("deviceType"),
      $"SupplyVendor",
      $"BidderCacheMachineName",
      $"AdjustedBidCPMInUSD",
      $"AuctionType",
      $"DealId",
      $"FirstPriceAdjustment".alias("bid_FirstPriceAdjustment"),
      $"FloorPriceInUSD",
      $"AdGroupId",
      $"PredictiveClearingRandomControl".alias("control_cohort"),
      $"TestId",
      when(
        $"TestId".like("%PCM(tfmodel%"), "plutus"
      ).when(
        $"testId".like("%PCM(tfmodel%bba%"), "plutus_with_basebid"
      )
        .when(
        $"PredictiveClearingMode.value" === 0, "legacy_pc_disabled"
      ).when(
        $"PredictiveClearingMode.value" === 1, "legacy_pc_miss"
      ).when(
        $"PredictiveClearingMode.value" === 3, "legacy_pc_hit"
      ).otherwise(
        "none"
      ).alias("model_simple")
    ) .filter(
      $"SupplyVendor".isin(sspList: _*) &&
        ($"BidderCacheMachineName".like("ca2%") || $"BidderCacheMachineName".like("de1%")))

    // MB2w from plutus raw
    // only available for google
  /*  val dfRaw = googleMinimumBidToWinData(date).select(
      "BidRequestId",
      "mb2w"
    )*/

    dfBids
      .join(
      dfImps,
      Seq("BidRequestId"),
      "left"
    )
      .withColumn(
        "has_deal",
        when($"DealId" =!= "", 1).otherwise(0)
      )
  }

  def oldVerticaQuery(dfFull: DataFrame) = {
    dfFull
      .groupBy(
      "model_simple",
      //"deviceType",
      //"has_deal",
      //"bid_price_range",
      //"control_cohort",
    ).agg(
      count("*").alias("bids"),
      sum(when($"RealMediaCostInUSD".isNotNull, 1).otherwise(0)).alias("imps"),

      mean($"AdjustedBidCPMInUSD").cast(DoubleType).alias("avg_bid_price"),
      mean($"RealMediaCostInUSD").cast(DoubleType).alias("avg_real_media_cost"),
      mean($"DiscrepancyMediaCost").cast(DoubleType).alias("avg_disc_media_cost"),

      mean(when(($"has_deal" =!= 1), $"RealMediaCostInUSD")).cast(DoubleType).alias("avg_non_deal_real_media_cost"),
      mean(when(($"has_deal" =!= 1), $"DiscrepancyMediaCost")).cast(DoubleType).alias("avg_non_deal_disc_media_cost"),

     // mean($"mb2w").alias("avg_mb2w"),


      mean($"FloorPriceInUSD").cast(DoubleType).alias("avg_floor_price"),

      //// Savings
      // this should be the difference between what we paid with plutus versus what we would have paid with discrepancy only
      sum(
        when($"RealMediaCostInUSD".isNotNull, $"DiscrepancyMediaCost" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("savings"),

      mean(
        when($"RealMediaCostInUSD".isNotNull, $"DiscrepancyMediaCost" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("avg_savings"),

      // total savings
      sum(
        when($"RealMediaCostInUSD".isNotNull, $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("savings_full"),

      mean(
        when($"RealMediaCostInUSD".isNotNull, $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("avg_savings_full"),

      mean(
        when($"RealMediaCostInUSD".isNotNull, lit(1.0) - ($"RealMediaCostInUSD" / $"AdjustedBidCPMInUSD"))
      ).cast(DoubleType).alias("avg_pushdown"),

      mean(
        when(($"has_deal" =!= 1) && ($"RealMediaCostInUSD".isNotNull), (lit(1.0) - ($"RealMediaCostInUSD" / $"AdjustedBidCPMInUSD")))
      ).cast(DoubleType).alias("avg_non_deal_pushdown"),


    ).withColumn("wr", $"imps" / $"bids"
    )
  }

  def adgroupLevel(dfFull: DataFrame) = {
    dfFull.groupBy(
      "AdGroupId"
    ).agg(
      mean(
        when(
          $"model_simple".like("plutus%") && $"RealMediaCostInUSD".isNotNull , $"DiscrepancyMediaCost" - $"RealMediaCostInUSD"
      )).cast(DoubleType).alias("plutus_savings_avg"),

      mean(
        when(
          ( $"model_simple".like("legacy%") && $"RealMediaCostInUSD".isNotNull ), $"DiscrepancyMediaCost" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("legacy_savings_avg"),

      mean(
        when(
           $"model_simple".like("plutus%") && ($"RealMediaCostInUSD".isNotNull ), $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("plutus_savings_full_avg"),

      mean(
        when(
           $"model_simple".like("legacy%") && $"has_deal" =!= 1 && ($"RealMediaCostInUSD".isNotNull ), $"DiscrepancyMediaCost" - $"RealMediaCostInUSD"
      )).cast(DoubleType).alias("legacy_savings_non_deal_avg"),

      mean(
        when(
           ($"model_simple".like("legacy%") && $"RealMediaCostInUSD".isNotNull ), $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD")
      ).cast(DoubleType).alias("legacy_savings_full_avg"),

      mean(
        when(
           ($"model_simple".like("plutus%")) && ($"has_deal" =!= 1) && ($"RealMediaCostInUSD".isNotNull ), $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD"
      )).cast(DoubleType).alias("plutus_savings_full_non_deal_avg"),

      mean(
        when(
          ($"model_simple".like("legacy%")) && ($"has_deal" =!= 1) && ($"RealMediaCostInUSD".isNotNull ), $"AdjustedBidCPMInUSD" - $"RealMediaCostInUSD"
      )).cast(DoubleType).alias("legacy_savings_full_non_deal_avg"),

   // ).withColumn("log_points_non_deal", log($"plutus_savings_non_deal_avg"/$"legacy_savings_non_deal_avg")
   // ).withColumn("log_points_non_deal_full", log($"plutus_savings_full_non_deal_avg"/$"legacy_savings_full_non_deal_avg")

    ).withColumn("log_points", log($"plutus_savings_avg"/$"legacy_savings_avg").cast(DoubleType)
    ).withColumn("log_points_full", log($"plutus_savings_full_avg"/$"legacy_savings_full_avg").cast(DoubleType))
  }

}
*/
