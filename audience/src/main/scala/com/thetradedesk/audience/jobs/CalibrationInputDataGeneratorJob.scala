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

object CalibrationInputDataGeneratorJob {
  val prometheus = new PrometheusClient("AudienceCalibrationDataJob", "RSMCalibrationInputDataGeneratorJob")


  def main(args: Array[String]): Unit = {
    runETLPipeline()
    prometheus.pushMetrics()
  }

  def runETLPipeline(): Unit = {
    RSMCalibrationInputDataGenerator.generateMixedOOSData(date)

  }
}


abstract class CalibrationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_calibration_input_data_generation_job_running_time", "RSMCalibrationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_calibration_input_data_generation_size", "RSMCalibrationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)


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
      if (df.columns.contains("FeatureValueHashed")) {
        df.withColumnRenamed("FeatureValueHashed", "SiteZipHashed")
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