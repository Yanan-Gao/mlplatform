package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.jobs.CalibrationInputDataGeneratorJob.prometheus
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase

case class Config(
                   model: String,
                   tag: String,
                   version: Int,
                   lookBack: Int,
                   startDate: LocalDate,
                   oosDataS3Bucket: String,
                   oosDataS3Path: String,
                   calibrationOutputData3Path: String,
                   subFolderKey: String,
                   subFolderValue: String,
                   date_time: String
                 )


object CalibrationInputDataGeneratorJob
  extends AutoConfigResolvingETLJobBase[Config](
    env = config.getString("confettiEnv", "prod"),
    experimentName = config.getStringOption("confettiExperiment"),
    groupName = "audience",
    jobName = "CalibrationInputDataGeneratorJob") {
  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceCalibrationDataJob", "RSMCalibrationInputDataGeneratorJob"))

  override def runETLPipeline(): Map[String, String] = {
    val conf = getConfig
    val dt = LocalDateTime.parse(conf.date_time)
    date = dt.toLocalDate
    dateTime = dt

    val jobConf = conf.copy(
      oosDataS3Bucket = S3Utils.refinePath(conf.oosDataS3Bucket),
      oosDataS3Path = S3Utils.refinePath(conf.oosDataS3Path),
      calibrationOutputData3Path = S3Utils.refinePath(conf.calibrationOutputData3Path)
    )
    RSMCalibrationInputDataGenerator.generateMixedOOSData(date, jobConf)
    Map("status" -> "success")
  }
}


abstract class CalibrationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_calibration_input_data_generation_job_running_time", "RSMCalibrationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_calibration_input_data_generation_size", "RSMCalibrationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)

  def generateMixedOOSData(date: LocalDate, conf: Config): Unit = {

    val start = System.currentTimeMillis()

    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val basePath = "s3://" + conf.oosDataS3Bucket + "/" + conf.oosDataS3Path
    val outputBasePath = "s3://" + conf.oosDataS3Bucket + "/" + conf.calibrationOutputData3Path

    val targetPath = constructPath(date, basePath)

    if (!pathExists(targetPath)(spark)) {
      throw new Exception(s"Target date path $targetPath does not exist.")
    }

    val candidateDates = (0 to conf.lookBack).map(days => date.minusDays(days)).filter(date => !date.isBefore(conf.startDate))

    val validPaths = candidateDates.flatMap { date =>
      val pathStr = constructPath(date, basePath)
      if (pathExists(pathStr)) Some(pathStr) else None
    }.reverse

    if (validPaths.isEmpty) {
      throw new Exception("No valid paths found within the lookback window.")
    }

    val validLookBackDays = validPaths.length - 1

    val weights = List(1 - 0.1 * (validLookBackDays)) ++ List.fill(validLookBackDays)(0.1)

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
      .save(s"$outputBasePath/${date.format(formatter)}000000/${conf.subFolderKey}=${conf.subFolderValue}")

    resultTableSize.labels(dateTime.toLocalDate.toString).set(result.count())
    jobRunningTime.labels(dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }

  def pathExists(pathStr: String)(implicit spark: SparkSession): Boolean = {
    FSUtils.directoryExists(pathStr)(spark)
  }

  def constructPath(date: LocalDate, basePath: String): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dayStr = date.format(formatter)
    s"$basePath/${dayStr}000000/split=OOS"
  }
}

object RSMCalibrationInputDataGenerator extends CalibrationInputDataGenerator(prometheus.get) {
}