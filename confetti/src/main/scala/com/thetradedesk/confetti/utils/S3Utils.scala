package com.thetradedesk.confetti.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import com.amazonaws.services.s3.model.ListObjectsV2Request

import scala.collection.JavaConverters._

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object S3Utils {
  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()

  def readFromS3(path: String): String = {
    val uri = new AmazonS3URI(path)
    val obj = s3Client.getObject(uri.getBucket, uri.getKey)
    scala.io.Source.fromInputStream(obj.getObjectContent).mkString
  }

  def writeToS3(path: String, data: String): Unit = {
    val uri = new AmazonS3URI(path)
    val bytes = data.getBytes(StandardCharsets.UTF_8)
    val is = new ByteArrayInputStream(bytes)
    s3Client.putObject(uri.getBucket, uri.getKey, is, null)
  }

  /** Check if an object exists at the given S3 path. */
  def exists(path: String): Boolean = {
    val uri = new AmazonS3URI(path)
    s3Client.doesObjectExist(uri.getBucket, uri.getKey)
  }

  /** Delete the object at the given S3 path. */
  def deleteFromS3(path: String): Unit = {
    val uri = new AmazonS3URI(path)
    s3Client.deleteObject(uri.getBucket, uri.getKey)
  }

  /** List YAML files under the given S3 folder path. */
  def listYamlFiles(folderPath: String): Seq[String] = {
    val uri = new AmazonS3URI(folderPath)
    val bucket = uri.getBucket
    val prefix = Option(uri.getKey).map(k => if (k.endsWith("/")) k else k + "/").getOrElse("")
    val req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)
    val result = s3Client.listObjectsV2(req)
    result.getObjectSummaries.asScala
      .map(_.getKey)
      .filter(k => k.toLowerCase.endsWith(".yml") || k.toLowerCase.endsWith(".yaml"))
      .map(k => s"s3://$bucket/$k")
      .toSeq
  }

}
