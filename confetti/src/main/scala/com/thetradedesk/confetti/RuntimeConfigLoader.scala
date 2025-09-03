package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

/**
 * Utility responsible for rendering runtime configuration templates and
 * writing the rendered results to hashed runtime paths on S3. The hash is
 * derived from the rendered identity configuration so that identical runtime
 * identities map to the same runtime location.
 */
class RuntimeConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {

  /** Render templates and upload them to a hashed runtime path.
   *
   * @param runtimeVars variables used to render template placeholders of the
   *                    form `{{var}}`
   * @return tuple containing the S3 prefix where rendered runtime configs were written
   *         and the combined rendered configuration in memory
   */
  def prepareRuntimeConfig(runtimeVars: Map[String, String]): (String, Map[String, String]) = {
    val templateDir = buildTemplateDir()
    val identityPath = templateDir + "identity_config.yml"
    val identityConfig = readYaml(identityPath)
    val renderedIdentity = renderRuntimeVariables(identityConfig, runtimeVars)
    checkForUnresolvedVariables(renderedIdentity)

    val yaml = new Yaml()
    val renderedIdentityStr = yaml.dump(renderedIdentity.asJava)
    val hash = HashUtils.sha256Base64(renderedIdentityStr)
    val runtimeBase = buildRuntimeBase(hash)
    val runtimeCfgKey = runtimeBase + "runtime_config.yml"
    S3Utils.writeToS3(runtimeCfgKey, renderedIdentityStr)

    val additionalConfigs = uploadAdditionalConfigs(templateDir, runtimeBase, runtimeVars)
    val configs = renderedIdentity ++ additionalConfigs
    (runtimeBase, configs)
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

  private def buildTemplateDir(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/"
  }

  private def buildRuntimeBase(hash: String): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime/$env/$expDir$groupName/$jobName/$hash/"
  }
}

