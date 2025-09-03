package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{CloudWatchLoggerFactory, Logger, LoggerFactory, MapConfigReader, S3Utils}
import com.thetradedesk.confetti.RuntimeConfigLoader
import org.apache.spark.sql.SparkSession
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
  private final def execute(): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
    try {
      val runtimeVars = Map.empty[String, String]
      val loader = new RuntimeConfigLoader(confettiEnv, experimentName, groupName, jobName, logger)
      val (runtimePathBase, identityCfg) = loader.renderIdentityConfig(runtimeVars)
      logger.info(s"Resolved runtime path base $runtimePathBase")

      if (loader.checkExistingRun(runtimePathBase, runtimeVars)) {
        return
      }

      val config = loader.loadRuntimeConfigs(runtimePathBase, identityCfg, runtimeVars)
      logger.info("Loaded runtime configuration")

      val successPath = runtimePathBase + "_SUCCESS"
      val runningPath = runtimePathBase + "_RUNNING"

      withRetry("write running marker to S3") {
        S3Utils.writeToS3(runningPath, experimentName.getOrElse(""))
      }
      logger.info(s"Wrote running marker to $runningPath")

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
      spark.stop()
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
