package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{CloudWatchLoggerFactory, Logger, LoggerFactory, MapConfigReader, S3Utils}
import com.thetradedesk.spark.util.TTDConfig.config
import org.yaml.snakeyaml.Yaml
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Base class for Confetti ETL jobs that automatically resolves configuration
 * before running the user defined ETL pipeline and writes its result to S3.
 */

abstract class AutoConfigResolvingETLJobBase[C: TypeTag : ClassTag](
    groupName: String,
    jobName: String,
    loggerFactory: LoggerFactory = CloudWatchLoggerFactory
  ) extends Serializable {
  @transient lazy val confettiEnv = config.getStringRequired("confettiEnv")
  @transient lazy val experimentName = config.getStringOption("experimentName")
  @transient lazy val runtimeConfigBasePath = config.getStringRequired("confettiRuntimeConfigBasePath")

  /** Optional Prometheus client for pushing metrics. */
  protected val prometheus: Option[PrometheusClient]

  @transient lazy val logger: Logger = loggerFactory.getLogger(
    s"Confetti-$confettiEnv",
    s"${experimentName.filter(_.nonEmpty).map(n => s"$n-").getOrElse("")}$groupName-$jobName"
  )
  private var jobConfig: Option[C] = None

  /**
   * Access the parsed configuration for the job. Throws an exception if
   * configuration has not been loaded.
   */
  protected final def getConfig: C =
    jobConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

  protected final def getLogger: Logger = logger

  /** Entry point for jobs extending this base. Executes the pipeline and pushes metrics. */
  final def main(args: Array[String]): Unit = {
    logger.info(s"Start executing: ${confettiEnv}-${experimentName}-${groupName}-${jobName}")
    execute()
    prometheus.foreach(_.pushMetrics())
  }

  /**
   * Run the ETL pipeline using the loaded config, exposure for user's implementation.
   */
  def runETLPipeline(): Unit

  /** Executes the job by loading configuration, running the pipeline and writing the results. */
  private val runtimePathBase: String =
    if (runtimeConfigBasePath.endsWith("/")) runtimeConfigBasePath else runtimeConfigBasePath + "/"

  private final def execute(): Unit = {
    val successPath = runtimePathBase + "_SUCCESS"
    val runningPath = runtimePathBase + "_RUNNING"
    var runningMarkerWritten = false
    try {
      if (withRetry("check success marker on S3") {
            S3Utils.exists(successPath)
          }) {
        logger.info(s"Success marker exists at $successPath; skipping execution")
        return
      }
      if (withRetry("check running marker on S3") {
            S3Utils.exists(runningPath)
          }) {
        val msg = s"Running marker exists at $runningPath; aborting job"
        logger.error(msg)
        throw new IllegalStateException(msg)
      }
      withRetry("write running marker to S3") {
        S3Utils.writeToS3(runningPath, experimentName.getOrElse(""))
      }
      runningMarkerWritten = true
      logger.info(s"Wrote running marker to $runningPath")

      val yamlPaths = withRetry("list YAML files from S3") {
        S3Utils.listYamlFiles(runtimePathBase)
      }
      val config = yamlPaths
        .map(readYaml)
        .foldLeft(Map.empty[String, String])(_ ++ _)
      logger.info(new Yaml().dump(config.asJava))
      jobConfig = Some(new MapConfigReader(config, logger).as[C])
      if (jobConfig.isEmpty) {
        throw new IllegalStateException("Config not initialized")
      }
      runETLPipeline()
      // Write a _SUCCESS file to signal job completion with experiment name
      withRetry("write success marker to S3") {
        S3Utils.writeToS3(successPath, experimentName.getOrElse(""))
      }
      logger.info(s"Wrote success marker to $successPath")
    } finally {
      if (runningMarkerWritten) {
        withRetry("delete running marker from S3") {
          S3Utils.deleteFromS3(runningPath)
        }
        logger.info(s"Deleted running marker at $runningPath")
      }
    }
  }

  /** Read a YAML file from S3 into a map. */
  private def readYaml(path: String): Map[String, String] = {
    val yamlStr = withRetry(s"read YAML from $path") {
      S3Utils.readFromS3(path)
    }
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    javaMap.asScala.map { case (k, v) => k -> v.toString }.toMap
  }

  /** Convert the map back to YAML and write it to the runtime config location. */
  private def writeYaml(config: Map[String, String], s3Path: String): Unit = {
    val yaml = new Yaml()
    val rendered = yaml.dump(config.asJava)
    withRetry(s"write YAML to $s3Path") {
      S3Utils.writeToS3(s3Path, rendered)
    }
  }

  /** Retry the given block up to 5 times with simple backoff and final error logging. */
  private def withRetry[T](operation: String, retries: Int = 5)(block: => T): T = {
    var attempt = 0
    var lastError: Throwable = null
    while (attempt < retries) {
      try {
        return block
      } catch {
        case e: Throwable =>
          lastError = e
          attempt += 1
          logger.warn(s"$operation failed on attempt $attempt: ${e.getMessage}")
          if (attempt < retries) {
            Thread.sleep(1000L * attempt)
          }
      }
    }
    logger.error(s"$operation failed after $retries attempts: ${lastError.getMessage}")
    throw lastError
  }

}
