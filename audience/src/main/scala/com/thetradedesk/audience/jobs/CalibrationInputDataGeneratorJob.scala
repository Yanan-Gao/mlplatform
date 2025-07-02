package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.{audienceResultCoalesce, ttdReadEnv, ttdWriteEnv}
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{shouldConsiderTDID3, _}
import com.thetradedesk.audience.jobs.CalibrationInputDataGeneratorJob.prometheus
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random
// import java.io.ObjectInputFilter.Config

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase

object CalibrationInputDataGeneratorJob
  extends AutoConfigResolvingETLJobBase(
    env = config.getString("confettiEnv", "prod"),
    experimentName = config.getStringOption("confettiExperiment"),
    groupName = "audience",
    jobName = "CalibrationInputDataGeneratorJob") {

  val prometheus = new PrometheusClient("AudienceCalibrationDataJob", "RSMCalibrationInputDataGeneratorJob")

  def main(args: Array[String]): Unit = {
    execute()
    prometheus.pushMetrics()
  }

  override def runETLPipeline(conf: Map[String, String]): Map[String, String] = {
    val dt = conf.get("date_time").map(LocalDateTime.parse).getOrElse(LocalDateTime.now())
    date = dt.toLocalDate
    dateTime = dt

    CalibrationInputDataGenerator.Config.load(conf)
    RSMCalibrationInputDataGenerator.generateMixedOOSData(date)
    Map("status" -> "success")
  }
}


abstract class CalibrationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_calibration_input_data_generation_job_running_time", "RSMCalibrationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_calibration_input_data_generation_size", "RSMCalibrationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)


  /*
  object Config {
    val model = config.getString("model", default = "RSMV2")
    val tag = config.getString("tag", default = "Seed_None")
    val version = config.getInt("version", default = 1)
    val lookBack = config.getInt("lookBack", default = 3)
    val startDate = config.getDate("startDate",  default = LocalDate.parse("2025-02-13"))
    val oosDataS3Bucket = S3Utils.refinePath(config.getString("oosDataS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val oosDataS3Path = S3Utils.refinePath(config.getString("oosDataS3Path", s"data/${ttdReadEnv}/audience/RSMV2/Seed_None/v=1"))
    val calibrationOutputData3Path = S3Utils.refinePath(config.getString("oosDataS3Path", s"data/${ttdWriteEnv}/audience/RSMV2/Seed_None/v=1"))
    val subFolderKey = config.getString("subFolderKey", default = "mixedForward")
    val subFolderValue = config.getString("subFolderValue", default = "Calibration")
  }
  */

  object Config {
    var model: String = "RSMV2"
    var tag: String = "Seed_None"
    var version: Int = 1
    var lookBack: Int = 3
    var startDate: LocalDate = LocalDate.parse("2025-02-13")
    var oosDataS3Bucket: String = S3Utils.refinePath("thetradedesk-mlplatform-us-east-1")
    var oosDataS3Path: String = S3Utils.refinePath(s"data/${ttdReadEnv}/audience/RSMV2/Seed_None/v=1")
    var calibrationOutputData3Path: String = S3Utils.refinePath(s"data/${ttdWriteEnv}/audience/RSMV2/Seed_None/v=1")
    var subFolderKey: String = "mixedForward"
    var subFolderValue: String = "Calibration"

    def load(map: Map[String, String]): Unit = {
      model = map.getOrElse("model", model)
      tag = map.getOrElse("tag", tag)
      version = map.get("version").map(_.toInt).getOrElse(version)
      lookBack = map.get("lookBack").map(_.toInt).getOrElse(lookBack)
      startDate = map.get("startDate").map(LocalDate.parse).getOrElse(startDate)
      oosDataS3Bucket = map.get("oosDataS3Bucket").map(S3Utils.refinePath).getOrElse(oosDataS3Bucket)
      oosDataS3Path = map.get("oosDataS3Path").map(S3Utils.refinePath).getOrElse(oosDataS3Path)
      calibrationOutputData3Path = map.get("calibrationOutputData3Path").map(S3Utils.refinePath).getOrElse(calibrationOutputData3Path)
      subFolderKey = map.getOrElse("subFolderKey", subFolderKey)
      subFolderValue = map.getOrElse("subFolderValue", subFolderValue)
    }
  }


  def generateMixedOOSData(date: LocalDate): Unit = {

    val start = System.currentTimeMillis()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val basePath = "s3://" + Config.oosDataS3Bucket + "/" + Config.oosDataS3Path
    val outputBasePath = "s3://" + Config.oosDataS3Bucket + "/" + Config.calibrationOutputData3Path

    val targetPath = constructPath(date, basePath)

    if (!pathExists(targetPath)(spark)) {
      throw new Exception(s"Target date path $targetPath does not exist.")
    }

    val candidateDates = (0 to Config.lookBack).map(days => date.minusDays(days)).filter(date => !date.isBefore(Config.startDate))

    val validPaths = candidateDates.flatMap { date =>
      val pathStr = constructPath(date, basePath)
      if (pathExists(pathStr)) Some(pathStr) else None
      }.reverse

    if (validPaths.isEmpty) {
      throw new Exception("No valid paths found within the lookback window.")
    }

    val validLookBackDays = validPaths.length - 1

    val weights = List(1-0.1*(validLookBackDays)) ++ List.fill(validLookBackDays)(0.1)

    val pathWeightPairs: Seq[(String, Double)] = validPaths.zip(weights)

    val dfs: Seq[DataFrame] = pathWeightPairs.map { case (pathStr, weight) => {
      val df = spark.read.format("tfrecord").load(pathStr).sample(withReplacement = false, fraction = weight)
      if (!df.columns.contains("AliasedSupplyPublisherIdCityHashed")) {
        // TODO: remove, temporary solution during transition to new density feature
        df.withColumn("AliasedSupplyPublisherIdCityHashed", lit(null).cast("long"))
      } else {
        df
      }
    }
    }
  
    val result = dfs.reduce(_.unionByName(_))
    
    result.coalesce(audienceResultCoalesce)
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(s"$outputBasePath/${date.format(formatter)}000000/${Config.subFolderKey}=${Config.subFolderValue}")

    resultTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }

  def pathExists(pathStr: String) (implicit spark: SparkSession): Boolean = {
      FSUtils.directoryExists(pathStr)(spark)
    }

  def constructPath(date: LocalDate, basePath: String): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dayStr = date.format(formatter)
    s"$basePath/${dayStr}000000/split=OOS"
  }
}

object RSMCalibrationInputDataGenerator extends CalibrationInputDataGenerator(prometheus: PrometheusClient) {
}