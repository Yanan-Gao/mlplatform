package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import org.yaml.snakeyaml.Yaml

import java.time.LocalDateTime
import scala.collection.JavaConverters._

class BehavioralConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {

  /**
    * Load behavioral configuration from S3, render runtime variables and write
    * the rendered configuration back to the runtime location.
    */
  def loadConfig(): Map[String, Any] = {
    val config = readConfigFromS3()
    val runtimeVars = Map("date_time" -> LocalDateTime.now().toString)
    val renderedConfig = renderRuntimeVariables(config, runtimeVars)
    writeRuntimeConfig(renderedConfig)
    renderedConfig
  }

  /** Read the YAML configuration from S3 and decode it into a map. */
  private def readConfigFromS3(): Map[String, Any] = {
    val path = buildConfigPath()
    val yamlStr = S3Utils.readFromS3(path)
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    javaMap.asScala.toMap[String, Any]
  }

  /**
    * Render runtime variables in the configuration.
    * @param config       the configuration map with placeholders
    * @param runtimeVars  values to substitute for placeholders like {{var}}
    */
  private def renderRuntimeVariables(
      config: Map[String, Any],
      runtimeVars: Map[String, String]
  ): Map[String, Any] = {
    def render(value: Any): Any = value match {
      case s: String =>
        runtimeVars.foldLeft(s) { case (acc, (k, v)) =>
          acc.replace(s"{{${k}}}", v)
        }
      case m: Map[_, _] =>
        renderRuntimeVariables(m.asInstanceOf[Map[String, Any]], runtimeVars)
      case it: Iterable[_] =>
        it.map(render).toList
      case other => other
    }
    config.map { case (k, v) => k -> render(v) }
  }

  /** Convert the map back to YAML and write it to the runtime config location. */
  private def writeRuntimeConfig(config: Map[String, Any]): Unit = {
    val yaml = new Yaml()
    val rendered = yaml.dump(config.asJava)
    val hash = HashUtils.sha256Base64(rendered)
    val runtimePath =
      s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$hash/behavioral_config.yml"
    S3Utils.writeToS3(runtimePath, rendered)
  }

  private def buildConfigPath(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/behavioral_config.yml"
  }

}
