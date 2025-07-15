package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.geronimo.shared.parquetDataPaths
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.opentelemetry.OtelClient
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter

object AvailsDealAggregate {
  val otelClient = new OtelClient("DealScoreJob", "AvailsDealAggregateJob")
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val tdids_by_dcsv_path = config.getString("tdids_by_dcsv_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_tdids/v=1")
  val users_scores_path = config.getString("users_scores_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid/v=1/date=${dateStr}/")
  val seed_id_path = config.getString(
    "seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/")

  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score/v=1/date=${dateStr}/")
  val agg_score_out_path = config.getString("agg_score_out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_agg/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )

  // Maximum and minimum number of users to output
  val cap_users = config.getInt("cap_users", 15000)
  val min_users = config.getInt("min_users", 3000)
  val lookback = config.getInt("look_back", 6)

  val dealCount = otelClient.createGauge(s"audience_deal_score_job_num_deal_svs", "Deal Score number unique DealCode supplyVendorIds")

  def runETLPipeline(): Unit = {

    val dealSv = spark.read.parquet(deal_sv_path)
    val dealSvSeedDS = dealSv.select("DealCode", "SupplyVendorId", "SeedId").distinct().cache();
    val dealSvDS = dealSvSeedDS.select("DealCode", "SupplyVendorId").distinct()
    dealCount.labels(Map("date" -> dateStr, "type" -> "total")).set(dealSvDS.count())

    val paths = parquetDataPaths(tdids_by_dcsv_path, date, None, Some(lookback))
    val TdidsByDCSV = spark.read.option("basePath", tdids_by_dcsv_path).parquet(paths: _*).select("TDID", "DealCode", "SupplyVendorId").distinct()
    val user_scores = spark.read.format("parquet").load(users_scores_path)
    val seedIds = spark.read.format("parquet").load(seed_id_path)

    val posexplodedSeedsWithRowNumber = seedIds.selectExpr("posexplode(SeedId) as (pos, SeedId)")

    // formula is 4 * cpu * number of machines
    val numberOfPartitions = num_workers * 4 * cpu_per_worker
    val TdidsWithScore = TdidsByDCSV.repartition(numberOfPartitions, col("TDID")).join(user_scores.repartition(numberOfPartitions, col("TDID")), Seq("TDID")).select(TdidsByDCSV("TDID"), TdidsByDCSV("DealCode"), TdidsByDCSV("SupplyVendorId"), user_scores("Score"))


    val TdisWithScoreWithSdf = TdidsWithScore.join(broadcast(dealSvSeedDS), trim(TdidsWithScore("DealCode")) === trim(dealSvSeedDS("DealCode")) && TdidsWithScore("SupplyVendorId") === dealSvSeedDS("SupplyVendorId"), "inner").select(col("TDID"), TdidsWithScore("DealCode"), TdidsWithScore("SupplyVendorId"), col("Score"), col("SeedId"))

    val finalDf = TdisWithScoreWithSdf.join(broadcast(posexplodedSeedsWithRowNumber), Seq("SeedId")).withColumn("Score", element_at(col("Score"), col("pos") + 1)).select(TdisWithScoreWithSdf("TDID"), TdisWithScoreWithSdf("DealCode"), TdisWithScoreWithSdf("SupplyVendorId"),  TdisWithScoreWithSdf("SeedId"), col("Score"))

    val structuredFinal = finalDf.withColumn("tdidValueStruct", struct(col("TDID"), col("Score")))

    val windowSpec = Window.partitionBy("DealCode", "SupplyVendorId", "SeedId").orderBy(col("rand"))
    val structuredFinalWithRowNum = structuredFinal
      .repartition(numberOfPartitions, col("DealCode"), col("SupplyVendorId"), col("SeedId"))
      .withColumn("rand", rand())
      .withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") <= cap_users)
      .drop("row_num")

    val raw_out = structuredFinalWithRowNum
      .groupBy("DealCode", "SupplyVendorId", "SeedId")
      .agg(collect_list(col("tdidValueStruct")).as("Scores"))
      //.withColumn("Scores", expr(s"slice(Scores, 1, $cap_users)"))

    raw_out
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(out_path)

    val raw_scores = spark.read.format("parquet").load(out_path)
    val dealSvSeed_avg_scores = raw_scores.withColumn("Score", explode($"Scores.Score"))
      .groupBy("DealCode", "SupplyVendorId", "SeedId")
      .agg(
        count("Score").as("UserCount"),
        avg("Score").as("AverageScore")
      ).cache()

    val found_deals = dealSvSeed_avg_scores.groupBy("DealCode", "SupplyVendorId")
      .agg(avg("UserCount").as("users")).cache()

    dealCount.labels(Map("date" -> dateStr, "type" -> "total")).set(dealSvDS.count())
    dealCount.labels(Map("date" -> dateStr, "type" -> "found")).set(found_deals.count())
    dealCount.labels(Map("date" -> dateStr, "type" -> "scored")).set(found_deals.filter(col("users") >= min_users).count())

    dealSvSeed_avg_scores
      .filter(col("UserCount") >= min_users)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(agg_score_out_path)

    otelClient.pushMetrics()

  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}

