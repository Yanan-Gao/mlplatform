package com.thetradedesk.confetti

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import org.yaml.snakeyaml.Yaml

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import scala.collection.JavaConverters._

/**
 * Base class for Confetti ETL jobs that automatically resolves configuration
 * before running the user defined ETL pipeline and writes its result to S3.
 */
abstract class AutoConfigResolvingETLJobBase(env: String,
                                             experimentName: Option[String],
                                             groupName: String,
                                             jobName: String) {

  private val loader = new BehaviroalConfigLoader(env, experimentName, groupName, jobName)
  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
  private var configHash: String = _

  /**
   * Load behavioral config and render runtime config using BehaviroalConfigLoader.
   */
  final def loadConfigAndRenderRuntimeConfig(): Map[String, Any] = {
    val config = loader.loadConfig()
    configHash = hashConfig(new Yaml().dump(config.asJava))
    config
  }

  /**
   * Run the ETL pipeline using the provided config.
   */
  def runETLPipeline(config: Map[String, String]): Map[String, String]

  /**
   * Write result map into Confetti runtime config folder.
   */
  final def writeResult(result: Map[String, String]): Unit = {
    val yaml = new Yaml()
    val rendered = yaml.dump(result.asJava)
    val runtimePath =
      s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$configHash/results.yml"
    writeToS3(runtimePath, rendered)
  }

  /** Executes the job by loading configuration, running the pipeline and writing the results. */
  final def execute(): Unit = {
    val config = loadConfigAndRenderRuntimeConfig().map { case (k, v) => k -> v.toString }
    val result = runETLPipeline(config)
    writeResult(result)
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
