package com.thetradedesk.audience.utils

import com.thetradedesk.audience.s3Client
import scala.io.Source

object SeedUtils {
  def queryCurrentDataVersion(s3Bucket: String, s3Path: String): String = {
    val s3Obj = s3Client.getObject(s3Bucket, s3Path + "/_CURRENT")
    Source.fromInputStream(s3Obj.getObjectContent).getLines.mkString.trim
  }
}
