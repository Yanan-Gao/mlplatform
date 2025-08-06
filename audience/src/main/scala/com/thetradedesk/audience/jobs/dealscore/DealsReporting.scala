package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.datasets.SupplyVendorDataSet
import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.datasets.sources.vertica.PerformanceReportDataSet
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.opentelemetry.{OtelClient, TtdGauge}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter


object DealsReporting {
  val otelClient = new OtelClient("DealScoreJob", "DealsReporting")
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val deal_score_path = config.getString("deal_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score/v=1/date=${dateStr}/")

  val overallGauge = otelClient.createGauge(s"audience_deal_score_report", "Deal Score pipeline report statistics against PerformanceReportDataSet")
  val min_users = config.getInt("min_users", 8000)


  private def aggregateAndCollect(df: DataFrame): Array[Row] = {
    df.agg(
      sum(col("PartnerSpendInUSD")).alias("TotalPartnerSpendInUSD"),
      sum(col("ImpressionCount")).alias("TotalImpressionCount"),
      sum(col("BidCount")).alias("TotalBidCount"),
      count_distinct(col("dealId"), col("SupplyVendorId")).alias("TotalDeals"))
      .collect()
  }

  private def updateOverallGauge(avails_stats: Array[Row],
                                 filterName: String,
                                 dateStr: String,
                                 overallGauge: TtdGauge
                        ): Unit = {
    if (avails_stats.nonEmpty) {
      val row = avails_stats(0)
      overallGauge.labels(Map("date" -> dateStr, "filter" -> filterName, "desc" -> "TotalPartnerSpendInUSD" )).set(row.getAs[java.math.BigDecimal]("TotalPartnerSpendInUSD").doubleValue())
      overallGauge.labels(Map("date" -> dateStr, "filter" -> filterName, "desc" -> "TotalImpressionCount")).set(row.getAs[Long]("TotalImpressionCount").longValue())
      overallGauge.labels(Map("date" -> dateStr, "filter" -> filterName, "desc" -> "TotalBidCount")).set(row.getAs[Long]("TotalBidCount").longValue())
      overallGauge.labels(Map("date" -> dateStr, "filter" -> filterName, "desc" -> "TotalDeals")).set(row.getAs[Long]("TotalDeals").longValue())
    }
  }

  def runETLPipeline(): Unit = {
    val svs = SupplyVendorDataSet().readPartition(date)(spark)

    val readParquetPerf =  PerformanceReportDataSet().readPartition(date)

    val dealSv = spark.read.parquet(deal_sv_path)
    val dealSvSeedDS = dealSv.select("DealCode", "SupplyVendorId", "SeedId").distinct();
    val dealSvDS = dealSvSeedDS.select("DealCode", "SupplyVendorId").distinct().cache()

    val deal_score = spark.read.parquet(deal_score_path)


    val performanceReport = readParquetPerf
      .groupBy(col("DealId"), col("SupplyVendor"))
      .agg(
        sum("PartnerCostInUSD").as("PartnerSpendInUSD"),
        sum("ImpressionCount").as("ImpressionCount"),
        sum("BidCount").as("BidCount"))

    val deal_spend = performanceReport
      .join(svs,
        lower(performanceReport("SupplyVendor")) === lower(svs("SupplyVendorName")), "left")
      .select(performanceReport("DealId"),
        performanceReport("SupplyVendor"),
        svs("SupplyVendorId"),
        performanceReport("PartnerSpendInUSD"),
        performanceReport("ImpressionCount"),
        performanceReport("BidCount") )

    val sent_deals_spend = deal_spend
      .join(dealSvDS, dealSvDS("DealCode") === deal_spend("DealId") &&
        dealSvDS("SupplyVendorId") === deal_spend("SupplyVendorId"), "left")
      .select(col("DealId"),
        deal_spend("SupplyVendorId"),
        col("PartnerSpendInUSD"),
        col("ImpressionCount"),
        col("BidCount"),
        col("DealCode"),
        dealSvDS("SupplyVendorId").alias("SupplyVendorId2"))
      .withColumn("Sent", col("DealCode").isNotNull)

    val foundDeals = deal_score.withColumn("Score", explode($"Scores.Score"))
      .groupBy("DealCode", "SupplyVendorId", "SeedId")
      .agg(
        count("Score").as("UserCount"),
        avg("Score").as("AverageScore")
      ).groupBy("DealCode", "SupplyVendorId")
      .agg(avg("UserCount").as("users"))
    foundDeals.cache()

    val scored_deals_spend = sent_deals_spend
      .join(foundDeals,
        foundDeals("DealCode") === sent_deals_spend("DealId") &&
        foundDeals("SupplyVendorId") === sent_deals_spend("SupplyVendorId"), "left")
      .select(sent_deals_spend("DealId"),
        sent_deals_spend("SupplyVendorId"),
        sent_deals_spend("PartnerSpendInUSD"),
        sent_deals_spend("ImpressionCount"),
        sent_deals_spend("BidCount"),
        foundDeals("users"),
        sent_deals_spend("DealCode"),
        col("Sent") )
      .cache()

    val avails_stats = aggregateAndCollect(scored_deals_spend)
    updateOverallGauge(avails_stats, "None", dateStr, overallGauge)

    val avails_with_deals_stats = aggregateAndCollect( scored_deals_spend.filter(col("DealId").isNotNull) )
    updateOverallGauge(avails_with_deals_stats, "HasDeal", dateStr, overallGauge)

    val deal_sent_stats =  aggregateAndCollect( scored_deals_spend.filter(col("Sent")) )
    updateOverallGauge(deal_sent_stats, "DealSent", dateStr, overallGauge)

    val scored = scored_deals_spend.filter(col("DealId").isNotNull && col("users") >= min_users)
    val scored_stats = aggregateAndCollect( scored )
    updateOverallGauge(scored_stats, "DealScored", dateStr, overallGauge)

    val not_scored = scored_deals_spend.filter(col("Sent") && ( col("users").isNull || col("users") < min_users ) )
    val unscored_stats = aggregateAndCollect( not_scored )
    updateOverallGauge(unscored_stats, "DealNotScored", dateStr, overallGauge)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    otelClient.pushMetrics()
  }


}
