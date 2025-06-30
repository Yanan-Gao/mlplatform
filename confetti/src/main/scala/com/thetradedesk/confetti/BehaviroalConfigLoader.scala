package com.thetradedesk.confetti

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import org.yaml.snakeyaml.Yaml

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import java.time.LocalDateTime
import scala.collection.JavaConverters._

class BehaviroalConfigLoader(env: String, experimentName: Option[String], groupName: String, jobName: String) {

  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()

  def loadConfig(): Map[String, Any] = {
    val path = buildConfigPath()
    val yamlStr = readFromS3(path)
    val yaml = new Yaml()
    val javaMap = yaml.load[java.util.Map[String, Any]](yamlStr)
    var config = javaMap.asScala.toMap[String, Any]

    // render runtime values
    config += ("date_time" -> LocalDateTime.now().toString)

    val rendered = yaml.dump(config.asJava)
    val hash = hashConfig(rendered)
    val runtimePath = s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/runtime-configs/$env/$groupName/$jobName/$hash/behavioral_config.yml"
    writeToS3(runtimePath, rendered)
    config
  }

  private def buildConfigPath(): String = {
    val expDir = experimentName.filter(_.nonEmpty).map(n => s"$n/").getOrElse("")
    s"s3://thetradedesk-mlplatform-us-east-1/configdata/confetti/configs/$env/$expDir$groupName/$jobName/behavioral_config.yml"
  }

  private def readFromS3(path: String): String = {
    val uri = new AmazonS3URI(path)
    val obj = s3Client.getObject(uri.getBucket, uri.getKey)
    scala.io.Source.fromInputStream(obj.getObjectContent).mkString
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
