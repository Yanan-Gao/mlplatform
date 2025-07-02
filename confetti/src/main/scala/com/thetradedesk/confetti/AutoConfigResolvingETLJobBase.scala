package com.thetradedesk.confetti

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import org.yaml.snakeyaml.Yaml
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
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
  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
  private var configHash: String = _
  private var internalConfig: Option[C] = None
  val prometheus = new PrometheusClient(
    prometheusAppName,
    prometheusJobName
  )

  /**
   * Load behavioral config and render runtime config using BehavioralConfigLoader.
   */
  private final def loadConfigAndRenderRuntimeConfig(): Unit = {
    val config = loader.loadConfig()
    configHash = hashConfig(new Yaml().dump(config.asJava))
    val stringMap = config.map { case (k, v) => k -> v.toString }
    internalConfig = Some(utils.ConfigFactory.fromMap[C](stringMap))
  }

  /**
   * Access the parsed configuration for the job. Throws an exception if
   * configuration has not been loaded.
   */
  protected final def getConfig: C =
    internalConfig.getOrElse(throw new IllegalStateException("Config not initialized"))

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
    writeToS3(runtimePath, rendered)
  }

  /** Executes the job by loading configuration, running the pipeline and writing the results. */
  final def execute(): Unit = {
    loadConfigAndRenderRuntimeConfig()
    if (internalConfig.isEmpty) {
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

  private def writeToS3(path: String, data: String): Unit = {
    val uri = new AmazonS3URI(path)
    val bytes = data.getBytes(StandardCharsets.UTF_8)
    val is = new ByteArrayInputStream(bytes)
    s3Client.putObject(uri.getBucket, uri.getKey, is, null)
  }

  private def hashConfig(content: String): String = {
    val digest = MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8))
    Base64.getEncoder.encodeToString(digest)
  }
}
