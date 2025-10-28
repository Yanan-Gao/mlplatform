package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{CloudWatchLoggerFactory, Logger, LoggerFactory, RuntimeConfigLoader}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

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
  // todo after fully onboard confetti, make this required, as well as runtimeConfigBasePath
  @transient lazy val confettiEnv = config.getString("confettiEnv", config.getString("ttd.env", "dev"))
  @transient lazy val experimentName = config.getStringOption("experimentName")
  @transient lazy val runtimeConfigBasePath = config.getStringOption("confettiRuntimeConfigBasePath")
  @transient lazy val manualConfetti = config.getBoolean("manualConfetti", default = false)

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

  /**
   * for backward compatibility, local test usage.
   * */
  def loadLegacyConfig(): C

  private final def execute(): Unit = {
//    val successPath = runtimePathBase + "_SUCCESS"
//    val runningPath = runtimePathBase + "_RUNNING"
//    if (withRetry("check success marker on S3") {
//          S3Utils.exists(successPath)
//        }) {
//      logger.info(s"Success marker exists at $successPath; skipping execution")
//      return
//    }
//    if (withRetry("check running marker on S3") {
//          S3Utils.exists(runningPath)
//        }) {
//      val msg = s"Running marker exists at $runningPath; aborting job"
//      logger.error(msg)
//      throw new IllegalStateException(msg)
//    }
//    withRetry("write running marker to S3") {
//      S3Utils.writeToS3(runningPath, experimentName.getOrElse(""))
//    }
//    logger.info(s"Wrote running marker to $runningPath")

    if(manualConfetti){
      val configLoader = new ManualConfigLoader[C](env = confettiEnv, experimentName = experimentName, groupName = groupName, jobName = jobName)
      jobConfig = Some(configLoader.loadRuntimeConfigs().config)
    }else if (runtimeConfigBasePath.exists(_.nonEmpty)) {
      // provided confetti runtime path
      val configLoader = new RuntimeConfigLoader[C](logger)
      jobConfig = Some(configLoader.loadConfig(runtimeConfigBasePath.get))
    } else {
      // doesn't provided confetti path. need to fall back to old way.
      jobConfig = Some(loadLegacyConfig())
    }

    if (jobConfig.isEmpty) {
      throw new IllegalStateException("Config not initialized")
    }
    runETLPipeline()
    // Write a _SUCCESS file to signal job completion with experiment name
//    withRetry("write success marker to S3") {
//      S3Utils.writeToS3(successPath, experimentName.getOrElse(""))
//    }
//    logger.info(s"Wrote success marker to $successPath")
  }

}
