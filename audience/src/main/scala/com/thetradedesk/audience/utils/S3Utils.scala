package com.thetradedesk.audience.utils

import com.thetradedesk.audience.s3Client

import scala.io.Source
import scala.util.{Failure, Success, Try}

object S3Utils {
  def queryCurrentDataVersion(s3Bucket: String, s3Path: String): String = {
    queryCurrentDataVersions(refinePath(s3Bucket), refinePath(s3Path)).mkString(",")
  }

  def queryCurrentDataVersions(s3Bucket: String, s3Path: String): Iterator[String] = {
    val content = Try(Source.fromInputStream(s3Client.getObject(refinePath(s3Bucket), refinePath(s3Path) + "/_CURRENT").getObjectContent))
    content match {
      case Failure(_) => Iterator.empty
      case Success(value) => {
        value.getLines.map(s => s.trim)
      }
    }
  }

  def updateCurrentDataVersion(s3Bucket: String, s3Path: String, versionContent: String): Unit = {
    s3Client.putObject(refinePath(s3Bucket), refinePath(s3Path) + "/_CURRENT", versionContent)
  }

  def refinePath(path: String): String = {
    path.stripPrefix("s3:").stripPrefix("s3a:").stripPrefix("s3n:").stripPrefix("/").stripSuffix("/")
  }
}
