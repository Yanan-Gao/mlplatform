package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.S3Utils
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

class BehavioralConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {

  /**
   * Load behavioral configuration from S3, render runtime variables and write
   * the rendered configuration back to the runtime location.
   */
  def loadRuntimeConfigs(runtimeVars: Map[String, String]): Map[String, String] = {
    val config = readConfigFromS3()
    val renderedConfig = renderRuntimeVariables(config, runtimeVars)
    checkForUnresolvedVariables(renderedConfig)
    renderedConfig
  }

  /** Read the YAML configuration from S3 and decode it into a map. */
  private def readConfigFromS3(): Map[String, String] = {
    val path = buildConfigPath()
    val yamlStr = S3Utils.readFromS3(path)
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    javaMap.asScala.map { case (k, v) => k -> v.toString }.toMap
  }

  /**
   * Render runtime variables in the configuration.
   *
   * @param config      the configuration map with placeholders
   * @param runtimeVars values to substitute for placeholders like {{var}}
   */
  private def renderRuntimeVariables(
                                      config: Map[String, String],
                                      runtimeVars: Map[String, String]
                                    ): Map[String, String] = {
    def render(value: String): String =
      runtimeVars.foldLeft(value) { case (acc, (k, v)) =>
        acc.replace(s"{{${k}}}", v)
      }
    // Nested map/list support is currently disabled but kept for potential future use
    /*
    def render(value: Any): Any = value match {
      case s: String => runtimeVars.foldLeft(s) { case (acc, (k, v)) => acc.replace(s"{{${k}}}", v) }
      case m: Map[_, _] => renderRuntimeVariables(m.asInstanceOf[Map[String, String]], runtimeVars)
      case it: Iterable[_] => it.map(render).toList
      case other => other
    }
    */
    config.map { case (k, v) => k -> render(v) }
  }

  /**
   * Validate that all configuration values are fully resolved.
   * Throws IllegalArgumentException if any value still contains
   * placeholder patterns like `{{var}}`.
   *
   * @param config configuration map to validate
   */
  private def checkForUnresolvedVariables(config: Map[String, String]): Unit = {
    val unresolved = config.filter { case (_, v) => v.contains("{{") && v.contains("}}") }
    if (unresolved.nonEmpty) {
      val entries = unresolved.map { case (k, v) => s"$k=$v" }.mkString(", ")
      throw new IllegalArgumentException(s"Config contains unresolved variables: $entries. " +
        s"This means that during this job runtime, the variables didn't get injected correctly.")
    }
  }

  private def buildConfigPath(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/behavioral_config.yml"
  }

}
