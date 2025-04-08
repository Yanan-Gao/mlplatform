package com.thetradedesk.audience.jobs

import com.amazonaws.services.s3.AmazonS3URI
import com.thetradedesk.audience.{date, dateFormatter, s3Client, ttdEnv}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TdidSeedScoreQualityCheck {
  val daysLookback = config.getInt("daysLookback", 1)
  val percentileDiffThreshold = config.getDouble("percentileDiffThreshold", 0.002f)
  val seedOutlinerPercentThreshold = config.getDouble("seedOutlinerPercentThreshold", 0.05f)

  val dateStr = date.format(dateFormatter)
  val population_score_path = config.getString("population_score_path", s"s3://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/scores/seedpopulationscore/v=1/date=${dateStr}/")
  val previousPaths = Stream.from(1).take(daysLookback).map(lb => {
    val dtStr = date.minusDays(lb).format(dateFormatter)
    population_score_path.replace(s"/date=${dateStr}/", s"/date=${dtStr}/")
  })

  val prometheus = new PrometheusClient("AudienceModelJob", "TdidSeedScoreQualityCheck")
  val numberOfOutliner = prometheus.createGauge(s"number_of_outliner_seed", "number of seeds that has obvious difference in p25 p50 or p75", "date")
  val percentOfOutliner = prometheus.createGauge(s"percent_of_outliner_seed", "percent of seeds that has obvious difference in p25 p50 or p75", "date")

  def pathExists(pathStr: String) (implicit spark: SparkSession): Boolean = {
    FSUtils.directoryExists(pathStr)(spark)
  }

  def uploadSccessFile(content: String):Unit = {
    val s3uri = new AmazonS3URI(population_score_path)
    s3Client.putObject(s3uri.getBucket, s3uri.getKey + "_SUCCESS", content)
  }
  /////
  def runETLPipeline(): Unit = {
    val previousDfsArr = previousPaths.map(p => {
      if (pathExists(p)) {
        spark.read.format("parquet").load(p)
      } else {
        null
      }
    }).filter(x => x != null)
    if (previousDfsArr.isEmpty) {
      // no old data, no op
      uploadSccessFile("found no previous date data")
      return
    }
    val previousDfs = previousDfsArr.reduce(_.union(_))
    val previousStats = previousDfs.select("SeedId", "PopulationSeedScoreRaw")
      .groupBy('SeedId)
      .agg(avg('PopulationSeedScoreRaw.alias("PopulationSeedScoreRawPre")))

    val todayStats = spark.read.format("parquet").load(population_score_path)
      .select("SeedId", "PopulationSeedScoreRaw")

    val result = todayStats.join(previousStats, Seq("SeedId"))
      .withColumn("diff", expr(s"if(abs(PopulationSeedScoreRawPre - PopulationSeedScoreRaw) > ${percentileDiffThreshold}, 1, 0)"))
      .agg(count("*").alias("total"), sum("diff").alias("outliners"))
      .collect()

    val totalRows = result(0).getAs[Long]("total")
    val outliners = result(0).getAs[Long]("outliners")
    if (totalRows > 0 && (outliners * 1.0 / totalRows < seedOutlinerPercentThreshold)) {
      uploadSccessFile("")
    } else {
      println(s"Failed data quality check, totalRows=${totalRows}, outliners=${outliners}")
    }

    numberOfOutliner.labels(dateStr).set(outliners)
    percentOfOutliner.labels(dateStr).set(if (totalRows > 0) outliners * 1.0 / totalRows else 0.0)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }
}
