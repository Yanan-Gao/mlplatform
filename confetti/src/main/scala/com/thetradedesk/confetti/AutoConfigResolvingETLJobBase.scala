package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import org.yaml.snakeyaml.Yaml
import com.thetradedesk.spark.util.prometheus.PrometheusClient

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

  /** Name of the Prometheus application. Must be provided by subclasses. */
  protected def prometheusAppName: String

  /** Name of the Prometheus job. Must be provided by subclasses. */
  protected def prometheusJobName: String

  private val loader = new BehavioralConfigLoader(env, experimentName, groupName, jobName)
  private var configHash: String = _
  private var jobConfig: Option[C] = None
  val prometheus = new PrometheusClient(
    prometheusAppName,
    prometheusJobName
  )

  /**
   * Load behavioral config and render runtime config using BehavioralConfigLoader.
   */
  private final def loadConfigAndRenderRuntimeConfig(): Unit = {
    val config = loader.loadConfig()
    configHash = HashUtils.sha256Base64(new Yaml().dump(config.asJava))
    val stringMap = config.map { case (k, v) => k -> v.toString }
    jobConfig = Some(utils.ConfigFactory.fromMap[C](stringMap))
  }

  /**
   * Access the parsed configuration for the job. Throws an exception if
   * configuration has not been loaded.
   */
  protected final def getConfig: C =
    jobConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

  /**
   * Run the ETL pipeline using the loaded config, exposure for user's implementation.
   */
  def runETLPipeline(): Map[String, String]

  /**
   * Write result map into Confetti runtime config folder.
   */
  private final def writeResult(result: Map[String, String]): Unit = {
    val yaml = new Yaml()
    val rendered = yaml.dump(result.asJava)
    val runtimePath =
      s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$configHash/results.yml"
    S3Utils.writeToS3(runtimePath, rendered)
  }

  /** Executes the job by loading configuration, running the pipeline and writing the results. */
  final def execute(): Unit = {
    loadConfigAndRenderRuntimeConfig()
    if (jobConfig.isEmpty) {
      throw new IllegalStateException("Config not initialized")
    }
    val result = runETLPipeline()
    writeResult(result)
  }

  /** Entry point for jobs extending this base. Executes the pipeline and pushes metrics. */
  final def main(args: Array[String]): Unit = {
    execute()
    prometheus.pushMetrics()
  }

}
