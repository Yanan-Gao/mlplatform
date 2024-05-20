package com.thetradedesk.featurestore.utils

import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.io.FSUtils

import scala.io.Source

object FileHelper {
  def readStringFromFile(path: String): String = {
    var content: String = null
    if (FSUtils.isLocalPath(path)(spark)) {
      content = Option(getClass.getResourceAsStream(path))
        .map { inputStream =>
          try {
            Source.fromInputStream(inputStream).getLines.mkString("\n")
          } finally {
            inputStream.close()
          }
        }.getOrElse(throw new IllegalArgumentException(s"Resource not found in JAR: $path"))
    } else {
      content = FSUtils.readStringFromFile(path)(spark)
    }
    content
  }
}