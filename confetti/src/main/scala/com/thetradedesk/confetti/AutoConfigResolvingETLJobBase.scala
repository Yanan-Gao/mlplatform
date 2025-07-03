package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{CloudWatchLogger, CloudWatchLoggerFactory, HashUtils, S3Utils}
import org.yaml.snakeyaml.Yaml
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDateTime
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Base class for Confetti ETL jobs that automatically resolves configuration
 * before running the user defined ETL pipeline and writes its result to S3.
 */

abstract class AutoConfigResolvingETLJobBase[C: TypeTag : ClassTag](env: String,
                                                                    experimentName: Option[String],
                                                                    groupName: String,
                                                                    jobName: String) {

  /** Optional Prometheus client for pushing metrics. */
  protected val prometheus: Option[PrometheusClient]

  private val loader = new BehavioralConfigLoader(env, experimentName, groupName, jobName)
  private val logger = CloudWatchLoggerFactory.getLogger(
    s"mlplatform-$env",
    s"${experimentName.filter(_.nonEmpty).map(n => s"$n-").getOrElse("")}$groupName-$jobName"
  )
  private var configHash: String = _
  private var jobConfig: Option[C] = None

  /**
   * Access the parsed configuration for the job. Throws an exception if
   * configuration has not been loaded.
   */
  protected final def getConfig: C =
    jobConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

  protected final def getLogger: CloudWatchLogger = logger

  /** Entry point for jobs extending this base. Executes the pipeline and pushes metrics. */
  final def main(args: Array[String]): Unit = {
    logger.info(s"Start executing: ${env}-${experimentName}-${groupName}-${jobName}")
    execute()
    prometheus.foreach(_.pushMetrics())
  }

  /**
   * Run the ETL pipeline using the loaded config, exposure for user's implementation.
   */
  def runETLPipeline(): Map[String, String]

  /** Executes the job by loading configuration, running the pipeline and writing the results. */
  private final def execute(): Unit = {
    // todo assemble runtime vars.
    val runtimeVars = Map("date_time" -> LocalDateTime.now().toString)

    val config = loader.loadRuntimeConfigs(runtimeVars)
    logger.info(new Yaml().dump(config.asJava))
    jobConfig = Some(utils.MapConfigReader.read[C](config))
    if (jobConfig.isEmpty) {
      throw new IllegalStateException("Config not initialized")
    }
    configHash = HashUtils.sha256Base64(new Yaml().dump(config.asJava))
    val runtimePathBase = s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$configHash/"
    writeYaml(config, runtimePathBase + "behavioral_config.yml")
    val result = runETLPipeline()
    writeYaml(result, runtimePathBase + "results.yml")
  }

  /** Convert the map back to YAML and write it to the runtime config location. */
  private def writeYaml(config: Map[String, String], s3Path: String): Unit = {
    val yaml = new Yaml()
    val rendered = yaml.dump(config.asJava)
    S3Utils.writeToS3(s3Path, rendered)
  }

}
