package com.thetradedesk.confetti

import com.thetradedesk.confetti.utils.{HashUtils, S3Utils}
import org.yaml.snakeyaml.Yaml

import java.time.LocalDateTime
import scala.collection.JavaConverters._

class BehavioralConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {


  def loadConfig(): Map[String, Any] = {
    val path = buildConfigPath()
    val yamlStr = S3Utils.readFromS3(path)
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    var config = javaMap.asScala.toMap[String, Any]

    // render runtime values
    config += ("date_time" -> LocalDateTime.now().toString)

    val rendered = yaml.dump(config.asJava)
    val hash = HashUtils.sha256Base64(rendered)
    val runtimePath = s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$hash/behavioral_config.yml"
    S3Utils.writeToS3(runtimePath, rendered)
    config
  }

  private def buildConfigPath(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/behavioral_config.yml"
  }

}
