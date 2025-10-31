package com.thetradedesk.audience.jobs

import com.thetradedesk.audience._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.paddingColumnsWithLength
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.confetti.AutoConfigResolvingETLJobBase
import com.thetradedesk.confetti.utils.Logger
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.{CBufferDataFrameReader, CBufferDataFrameWriter}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class CalibrationInputDataGeneratorJobConfig(
                                                   model: String,
                                                   tag: String,
                                                   version: Int,
                                                   lookBack: Int,
                                                   runDate: LocalDate,
                                                   startDate: LocalDate,
                                                   oosDataS3Bucket: String,
                                                   oosDataS3Path: String,
                                                   subFolderKey: String,
                                                   subFolderValue: String,
                                                   oosProdDataS3Path: String,
                                                   coalesceProdData: Boolean,
                                                   audienceResultCoalesce: Int,
                                                   outputPath: String,
                                                   outputCBPath: String
                                                 )

object CalibrationInputDataGeneratorJob
  extends AutoConfigResolvingETLJobBase[CalibrationInputDataGeneratorJobConfig](
    groupName = "audience",
    jobName = "CalibrationInputDataGeneratorJob") {
  override val prometheus: Option[PrometheusClient] =
    Some(new PrometheusClient("AudienceCalibrationDataJob", "RSMCalibrationInputDataGeneratorJob"))

  override def runETLPipeline(): Unit = {
    val conf = getConfig

    val jobConf = conf.copy(
      oosDataS3Bucket = S3Utils.refinePath(conf.oosDataS3Bucket),
      oosDataS3Path = S3Utils.refinePath(conf.oosDataS3Path),
    )
    val logger = getLogger
    logger.info("Going to trigger calibration input job. Finished the config resolving")
    RSMCalibrationInputDataGenerator.generateMixedOOSData(conf.runDate, jobConf, logger)
  }

  /**
   * for backward compatibility, local test usage.
   * */
  override def loadLegacyConfig(): CalibrationInputDataGeneratorJobConfig = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val oosDataS3Bucket = S3Utils.refinePath(config.getString("oosDataS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val oosDataS3Path = S3Utils.refinePath(config.getString("oosDataS3Path", s"data/${ttdReadEnv}/audience/RSMV2/Seed_None/v=2"))
    val calibrationOutputData3Path = S3Utils.refinePath(config.getString("calibrationOutputData3Path", s"data/${ttdWriteEnv}/audience/RSMV2/Seed_None"))

    val outputBasePath = "s3://" + oosDataS3Bucket + "/" + calibrationOutputData3Path

    val subFolderKey = config.getString("subFolderKey", default = "mixedForward")
    val subFolderValue = config.getString("subFolderue", default = "Calibration")

    CalibrationInputDataGeneratorJobConfig(
      model = config.getString("model", default = "RSMV2"),
      tag = config.getString("tag", default = "Seed_None"),
      version = config.getInt("version", default = 1),
      lookBack = config.getInt("lookBack", default = 3),
      coalesceProdData = config.getBoolean("coalesceProdData", default = false), // for testing and experiment, when data is missing in ttdReadEnv, use prod data
      startDate = config.getDate("startDate", default = LocalDate.parse("2025-02-13")),
      oosDataS3Bucket = oosDataS3Bucket,
      oosDataS3Path = oosDataS3Path,
      subFolderKey = subFolderKey,
      subFolderValue = subFolderValue,
      audienceResultCoalesce = config.getInt("audienceResultCoalesce", 4096),
      outputPath = s"$outputBasePath/v=1/${date.format(formatter)}000000/${subFolderKey}=${subFolderValue}",
      outputCBPath = s"$outputBasePath/v=2/${date.format(formatter)}000000/${subFolderKey}=${subFolderValue}",
      oosProdDataS3Path = config.getString("oosProdDataS3Path", default = "data/prod/audience/RSMV2/Seed_None/v=1"),
      runDate = date
    )
  }
}


abstract class CalibrationInputDataGenerator(prometheus: PrometheusClient) {

  val jobRunningTime = prometheus.createGauge(s"audience_calibration_input_data_generation_job_running_time", "RSMCalibrationInputDataGenerator running time", "date")
  val resultTableSize = prometheus.createGauge(s"audience_calibration_input_data_generation_size", "RSMCalibrationInputDataGenerator table size", "date")
  val sampleUDF = shouldConsiderTDID3(config.getInt("hitRateUserDownSampleHitPopulation", default = 1000000), config.getString("saltToSampleHitRate", default = "0BgGCE"))(_)

  def generateMixedOOSData(date: LocalDate, conf: CalibrationInputDataGeneratorJobConfig, logger: Logger): Unit = {

    logger.info("Start.")

    val start = System.currentTimeMillis()

    val basePath = "s3://" + conf.oosDataS3Bucket + "/" + conf.oosDataS3Path

    val targetPath = constructPath(date, basePath)

    if (!pathExists(targetPath)(spark)) {
      logger.error(s"Target date path $targetPath does not exist.")
      throw new Exception(s"Target date path $targetPath does not exist.")
    }

    val candidateDates = (0 to conf.lookBack).map(days => date.minusDays(days)).filter(date => !date.isBefore(conf.startDate))

    val validPaths = candidateDates.flatMap { date =>
      val pathStr = constructPath(date, basePath)
      if (pathExists(pathStr)) Some(pathStr) else None
    }.reverse

    if (validPaths.isEmpty) {
      logger.error("No valid paths found within the lookback window.")
      throw new Exception("No valid paths found within the lookback window.")
    }

    val validLookBackDays = validPaths.length - 1

    val weights = List(1 - 0.1 * (validLookBackDays)) ++ List.fill(validLookBackDays)(0.1)

    val pathWeightPairs: Seq[(String, Double)] = validPaths.zip(weights)

    val dfs: Seq[DataFrame] = pathWeightPairs.map { case (pathStr, weight) => {

      val df = spark.read.cb(pathStr).sample(withReplacement = false, fraction = weight)
      // backward compatibility, to be cleaned up 
      if (df.columns.contains("FeatureValueHashed")) {
        df.withColumnRenamed("FeatureValueHashed", "SiteZipHashed")
      } else {
        df
      }

      val potentiallyMissingColumns = Map(
        "AliasedSupplyPublisherIdCityHashed" -> "long",
        "IdTypesBitmap" -> "int",
        "CookieTDID" -> "string",
        "DeviceAdvertisingId" -> "string",
        "IdentityLinkId" -> "string",
        "EUID" -> "string",
        "UnifiedId2" -> "string",
      )

      addMissingColumns(df, potentiallyMissingColumns)
    }
    }
    logger.info("Before get results...")

    val colMaxLength = Map("MatchedSegments" -> 200)
    val result = paddingColumnsWithLength(
      dfs.reduce(_.unionByName(_)), colMaxLength, 0)
      .cache

    logger.info("Finished everything, trying to produce results...")

    result.coalesce(conf.audienceResultCoalesce)
      .write.mode(SaveMode.Overwrite)
      .option("maxChunkRecordCount", 20480)
      .cb(conf.outputCBPath)

    result.coalesce(conf.audienceResultCoalesce)
      .write.mode(SaveMode.Overwrite)
      .format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(conf.outputPath)

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

  def addMissingColumns(df: DataFrame, columnSpec: Map[String, String]): DataFrame = {
    columnSpec.foldLeft(df) { case (df, (colName, typeName)) =>
      if (df.columns.contains(colName)) {
        df
      } else {
        df.withColumn(colName, lit(null).cast(typeName))
      }
    }
  }

}

object RSMCalibrationInputDataGenerator extends CalibrationInputDataGenerator(CalibrationInputDataGeneratorJob.prometheus.get) {
}