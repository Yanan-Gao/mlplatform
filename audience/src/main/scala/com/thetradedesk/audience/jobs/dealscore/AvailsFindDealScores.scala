package com.thetradedesk.audience.jobs.dealscore

import com.thetradedesk.audience.{date, dateFormatter, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter

object AvailsFindDealScores {
  val dateStr = date.format(dateFormatter)
  val dateStrDash = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val deal_sv_path = config.getString("deal_sv_path", s"s3://ttd-deal-quality/env=prod/VerticaExportDealRelevanceSeedMapping/VerticaAws/date=${dateStr}/")
  val out_path = config.getString("out_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/deal_score_tdids/v=1/date=${dateStr}/")

  val num_workers = config.getInt("num_workers", 200 )
  val cpu_per_worker = config.getInt("cpu_per_worker", 32 )
  val findDealsCoalesce = config.getInt("findDealsCoalesce", 4096)

  val samplingRate = config.getInt("sampling_rate", 3)
  val min_hour = config.getInt("min_hour", 0)
  val max_hour = config.getInt("max_hour", 24)
  val limit_users = config.getInt("limit_users", 50000)


  def runETLPipeline(): Unit = {

    val dealSv = spark.read.parquet(deal_sv_path)
    val dealSvSeedDS = dealSv.select("DealCode", "SupplyVendorId", "SeedId").distinct().cache();
    val dealSvDS = dealSvSeedDS.select("DealCode", "SupplyVendorId").distinct()

    val avails = DealScoreUtils.loadAvailsData(dateStrDash, min_hour, max_hour)

    val availsProcessed = DealScoreUtils
      .processUserIdentifiers(avails, samplingRate, Seq("UserIdentifiers", "DealCodes", "SupplyVendorId"))
      .filter(size(col("DealCodes")) > 0 && !col("DealCodes")(0).isNull && col("DealCodes")(0) =!= "")

    val TdidsByDCSV = DealScoreUtils.explodeJoinAndDistinct(
      sourceDF = availsProcessed,
      explodeColumns = Map("Identifiers" -> "TDID", "DealCodes" -> "DealCode"),
      selectAfterExplode = Seq("TDID", "DealCode", "SupplyVendorId"),
      joinDF = dealSvDS,
      joinColumns = Seq("DealCode", "SupplyVendorId")
    )

    val numberOfPartitions = num_workers * 4 * cpu_per_worker
    DealScoreUtils.limitAndSave(
      sourceDF = TdidsByDCSV,
      partitionColumns = Seq("DealCode", "SupplyVendorId"),
      numberOfPartitions = numberOfPartitions,
      limitUsers = limit_users,
      coalesceTo = findDealsCoalesce,
      outPath = out_path
    )
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}
