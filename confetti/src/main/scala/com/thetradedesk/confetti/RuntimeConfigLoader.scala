package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, Logger, S3Utils}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

/**
 * Utility responsible for rendering runtime configuration templates and
 * writing the rendered results to hashed runtime paths on S3. The hash is
 * derived from the rendered identity configuration so that identical runtime
 * identities map to the same runtime location.
 */
class RuntimeConfigLoader(
    env: String,
    experimentName: Option[String],
    groupName: String,
    jobName: String,
    logger: Logger
) {

  /** Render the identity config and return the runtime base path without writing anything. */
  def renderIdentityConfig(runtimeVars: Map[String, String]): (String, Map[String, String]) = {
    val templateDir = buildTemplateDir()
    val identityPath = templateDir + "identity_config.yml"
    val identityConfig = readYaml(identityPath)
    val renderedIdentity = renderRuntimeVariables(identityConfig, runtimeVars)
    checkForUnresolvedVariables(renderedIdentity)

    val yaml = new Yaml()
    val renderedIdentityStr = yaml.dump(renderedIdentity.asJava)
    val hash = HashUtils.sha256Base64(renderedIdentityStr)
    val runtimeBase = buildRuntimeBase(hash)
    (runtimeBase, renderedIdentity)
  }

  /** Write all runtime configs to S3 under the given runtime base. */
  def writeRuntimeConfigs(
      runtimeBase: String,
      identityConfig: Map[String, String],
      runtimeVars: Map[String, String]
  ): Map[String, String] = {
    val yaml = new Yaml()
    val runtimeCfgKey = runtimeBase + "runtime_config.yml"
    S3Utils.writeToS3(runtimeCfgKey, yaml.dump(identityConfig.asJava))

    val templateDir = buildTemplateDir()
    val additionalConfigs = uploadAdditionalConfigs(templateDir, runtimeBase, runtimeVars)
    identityConfig ++ additionalConfigs
  }

  /** Check for existing runs via S3 markers and perform fast-pass copy when appropriate. */
  def checkExistingRun(
      runtimeBase: String,
      runtimeVars: Map[String, String]
  ): Boolean = {
    val successPath = runtimeBase + "_SUCCESS"
    val runningPath = runtimeBase + "_RUNNING"

    if (withRetry("check success marker on S3") { S3Utils.exists(successPath) }) {
      logger.info(s"Success marker exists at $successPath; skipping execution")
      copyPreviousOutputs(runtimeBase, runtimeVars)
      true
    } else if (withRetry("check running marker on S3") { S3Utils.exists(runningPath) }) {
      logger.warn(s"Running marker exists at $runningPath; waiting for completion")
      val timeoutMillis = 2 * 60 * 60 * 1000L // 2 hours
      val pollIntervalMillis = 30000L
      val startTime = System.currentTimeMillis()
      var done = false
      while (!done && System.currentTimeMillis() - startTime < timeoutMillis) {
        Thread.sleep(pollIntervalMillis)
        done = withRetry("check success marker on S3") { S3Utils.exists(successPath) }
      }
      if (done) {
        logger.info(s"Success marker exists at $successPath; skipping execution")
        copyPreviousOutputs(runtimeBase, runtimeVars)
        true
      } else {
        val msg = s"Running marker exists at $runningPath; aborting job after timeout"
        logger.error(msg)
        throw new IllegalStateException(msg)
      }
    } else {
      false
    }
  }

  /** Upload all configs other than the identity config to the runtime path. */
  private def uploadAdditionalConfigs(
      templateDir: String,
      runtimeBase: String,
      runtimeVars: Map[String, String]
  ): Map[String, String] = {
    val templates = S3Utils.listYamlFiles(templateDir)
    templates
      .filterNot(_.endsWith("identity_config.yml"))
      .map { path =>
        val name = path.split("/").last
        val cfg = readYaml(path)
        val rendered = renderRuntimeVariables(cfg, runtimeVars)
        checkForUnresolvedVariables(rendered)
        val yaml = new Yaml()
        val renderedStr = yaml.dump(rendered.asJava)
        S3Utils.writeToS3(runtimeBase + name, renderedStr)
        rendered
      }
      .foldLeft(Map.empty[String, String])(_ ++ _)
  }

  /** Read a YAML file from S3 into a map of string values. */
  private def readYaml(path: String): Map[String, String] = {
    val yamlStr = S3Utils.readFromS3(path)
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    javaMap.asScala.map { case (k, v) => k -> v.toString }.toMap
  }

  /** Replace placeholders in the config with provided runtime variables. */
  private def renderRuntimeVariables(config: Map[String, String], runtimeVars: Map[String, String]): Map[String, String] = {
    def render(value: String): String =
      runtimeVars.foldLeft(value) { case (acc, (k, v)) => acc.replace(s"{{${k}}}", v) }
    config.map { case (k, v) => k -> render(v) }
  }

  /** Ensure no placeholders remain unresolved. */
  private def checkForUnresolvedVariables(config: Map[String, String]): Unit = {
    val unresolved = config.filter { case (_, v) => v.contains("{{") && v.contains("}}") }
    if (unresolved.nonEmpty) {
      val entries = unresolved.map { case (k, v) => s"$k=$v" }.mkString(", ")
      throw new IllegalArgumentException(s"Config contains unresolved variables: $entries")
    }
  }

  /** Copy previous job outputs to the current output paths. */
  private def copyPreviousOutputs(
      runtimeBase: String,
      runtimeVars: Map[String, String]
  ): Unit = {
    val templateDir = buildTemplateDir()
    val outTemplatePath = templateDir + "output_config.yml"
    val outTemplate = readYaml(outTemplatePath)
    val currentCfg = renderRuntimeVariables(outTemplate, runtimeVars)
    checkForUnresolvedVariables(currentCfg)
    val prevCfgPath = runtimeBase + "output_config.yml"
    val prevCfg = readYaml(prevCfgPath)
    if (currentCfg.keySet != prevCfg.keySet) {
      throw new IllegalArgumentException("Output config keys do not match previous run")
    }
    prevCfg.foreach { case (k, prevPath) =>
      val currPath = currentCfg(k)
      try {
        val data = S3Utils.readFromS3(prevPath)
        S3Utils.writeToS3(currPath, data)
      } catch {
        case e: Throwable =>
          logger.warn(s"Failed to copy $prevPath to $currPath: ${e.getMessage}")
      }
    }
  }

  private def buildTemplateDir(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/"
  }

  private def buildRuntimeBase(hash: String): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime/$env/$expDir$groupName/$jobName/$hash/"
  }

  /** Retry helper for S3 operations. */
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
          if (attempt < retries) {
            Thread.sleep(1000L * attempt)
          }
      }
    }
    throw lastError
  }
}


