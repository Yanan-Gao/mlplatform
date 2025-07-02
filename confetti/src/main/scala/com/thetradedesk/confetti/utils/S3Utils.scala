package com.thetradedesk.confetti.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}

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

}
