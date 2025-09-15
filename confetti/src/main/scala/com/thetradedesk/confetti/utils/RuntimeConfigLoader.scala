package com.thetradedesk.confetti.utils

import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._

/**
 * Utility for loading job config of type C from S3 YAML files.
 *
 * @param logger Logger instance used for diagnostics.
 * @tparam C Target config case class type.
 */
class RuntimeConfigLoader[C](logger: Logger)(implicit m: Manifest[C]) {

  /**
   * Load runtime job configuration from S3 YAML files.
   *
   * @param runtimeConfigBasePath Base S3 path where YAMLs are stored (must not be empty).
   * @return Parsed config instance of type C.
   */
  def loadConfig(runtimeConfigBasePath: String): C = {
    // normalize the path
    val runtimePathBase =
      if (runtimeConfigBasePath.endsWith("/")) runtimeConfigBasePath
      else runtimeConfigBasePath + "/"

    val yamlPaths = withRetry("list YAML files from S3") {
      S3Utils.listYamlFiles(runtimePathBase)
    }

    val config = yamlPaths
      .map(readYaml)
      .foldLeft(Map.empty[String, String])(_ ++ _)

    logger.info(new Yaml().dump(config.asJava))

    new MapConfigReader(config, logger).as[C]
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
