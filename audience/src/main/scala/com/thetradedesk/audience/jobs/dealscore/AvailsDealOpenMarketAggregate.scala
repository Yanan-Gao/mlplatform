package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.jobs.dealscore.DealScoreUtils._
import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.opentelemetry.OtelClient
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter

object AvailsDealOpenMarketAggregate {
  val otelClient = new OtelClient("DealScoreJob", "AvailsDealAggregateJob")
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val tdids_by_pc_path = config.getString("tdids_by_pc_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_tdids/v=1")
  val users_scores_path = config.getString("users_scores_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/tdid2seedid/v=1/date=${dateStr}/")
  val seed_id_path = config.getString(
    "seed_id_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedids/v=2/date=${dateStr}/")
  val deals_OM_equivalents_path  = config.getString("deals_OM_equivalents_path", s"s3://ttd-deal-quality/env=prod/DealQualityMetrics/DealOpenMarketEquivalents/v=1/date=${dateStr}/" )

  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score/v=1/date=${dateStr}/")
  val agg_score_out_path = config.getString("agg_score_out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_agg/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )

  // Maximum and minimum number of users to output
  val cap_users = config.getInt("cap_users", 15000)
  val min_users = config.getInt("min_users", 8000)
  val lookback = config.getInt("look_back", 6)
  val partitionPerCore = config.getInt("partition_per_core", 1)
  // This multiplier is used for multiplying the partitions when collecting at the ent to ease memory usage.
  val partitionMultiplier = config.getInt("partition_mult", 1)

  val min_adInfoCount = config.getInt("min_adInfoCount", 100000)
  
  // Aggregation dimensions configuration
  val aggregation_mode = config.getString("aggregation_mode", "full") // "full" or "property_country"

  // Helper functions to get aggregation dimensions based on mode
  def getPartitionColumns(aggregationMode: String): Seq[String] = {
    aggregationMode match {
      case "propertyId_country" => Seq("PropertyIdString", "Country")
      case _ => Seq("InventoryChannel", "PropertyIdString", "Country") // default "full" mode
    }
  }

  def runETLPipeline(): Unit = {

    val numberOfPartitions = num_workers * partitionPerCore * cpu_per_worker
    val dealSv = spark.read.parquet(deal_sv_path)
    //hardcode for now, filter out low volume combos
    val deals_om_equivalent = spark.read.parquet(deals_OM_equivalents_path)
      .filter(col("AdInfoCounts") > min_adInfoCount)
      .withColumnRenamed("PropertyId", "PropertyIdString")
      .repartition(numberOfPartitions, col("SupplyVendorId"), col("DealCode"))
    val deal_open_market = deals_om_equivalent.select((getPartitionColumns(aggregation_mode) :+ "SupplyVendorId" :+ "DealCode").map(col): _*).distinct().join(dealSv, Seq("SupplyVendorId", "DealCode"))
    val pc_om_seed = deal_open_market.select((getPartitionColumns(aggregation_mode) :+ "SeedId").map(col): _*).distinct()//.cache()


    DealScoreUtils.getScoredTdids(
      tdidsByPropsPath = tdids_by_pc_path,
      usersScoresPath = users_scores_path,
      numberOfPartitions = numberOfPartitions,
      capUsers = cap_users,
      date = date,
      lookback = lookback,
      columns = getPartitionColumns(aggregation_mode),
      keepScores = false).capScoredUsers(
        numberOfPartitions = numberOfPartitions,
        capUsers = cap_users,
        columns = getPartitionColumns(aggregation_mode),
        seedScorePresent = false
      ).joinWithSeedData(
        seedsToScore = pc_om_seed,
        partitionColumns = getPartitionColumns(aggregation_mode),
        hasScoreColumn = false,
        broadcastSeedData = false
      ).joinWithUserScore(
        partitionColumns =  getPartitionColumns(aggregation_mode),
        usersScoresPath =  users_scores_path,
        numberOfPartitions =  numberOfPartitions,
      )
      .extractSeedScore(
        partitionColumns = getPartitionColumns(aggregation_mode),
        seedIdPath = seed_id_path
      ).collectScores(
        partitionColumns = getPartitionColumns(aggregation_mode),
        numberOfPartitions =  numberOfPartitions * partitionMultiplier
      )
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(out_path)

    val raw_scores = spark.read.format("parquet").load(out_path)
    val dealSvSeed_avg_scores = raw_scores.withColumn("Score", explode($"Scores.Score"))
      .groupBy((getPartitionColumns(aggregation_mode) :+ "SeedId").map(col): _*)
      .agg(
        count("Score").as("UserCount"),
        avg("Score").as("AverageScore")
      )


    dealSvSeed_avg_scores
      .filter(col("UserCount") >= min_users)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(agg_score_out_path)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
