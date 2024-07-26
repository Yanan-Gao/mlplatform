package com.thetradedesk.featurestore.utils

import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.io.FSUtils.isLocalPath
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{ChecksumFileSystem, FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.IOException
import java.net.URI
import java.nio.charset.StandardCharsets
import scala.io.Source

object FileHelper {
  def readStringFromFile(filePath: String, fileInResource: Boolean = false)(implicit sparkSession: SparkSession): String = {
    val path = databricksHack(filePath)
    var content: String = null
    if (fileInResource && FSUtils.isLocalPath(path)(sparkSession)) {
      content = Option(getClass.getResourceAsStream(path))
        .map { inputStream =>
          try {
            Source.fromInputStream(inputStream).getLines.mkString("\n")
          } finally {
            inputStream.close()
          }
        }.getOrElse(throw new IllegalArgumentException(s"Resource not found in JAR: $path"))
    } else {
      content = FSUtils.readStringFromFile(path)
    }
    content
  }

  private def getFileSystem(path: String)(implicit spark: SparkSession): FileSystem = {
    if (spark == null) throw new IllegalArgumentException("Spark session can't be null!")
    if (spark.sparkContext == null) throw new IllegalArgumentException("Spark context can't be null!")
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    if (hadoopConfig == null) throw new IllegalArgumentException("Hadoop configuration can't be null!")
    if (isLocalPath(path)) {
      FileSystem.getLocal(hadoopConfig)
    }
    else {
      FileSystem.get(new URI(path), hadoopConfig)
    }
  }

  // hack code to fix filesystem flags bug in FSUtils
  final def writeStringToFile(filePath: String, content: String)(implicit sparkSession: SparkSession): Unit = {
    val file = databricksHack(filePath)
    val fileSystem = getFileSystem(file)
    val writeChecksum = tryReadFileSystemFlag(fileSystem, "writeChecksum")
    val verifyChecksum = tryReadFileSystemFlag(fileSystem, "verifyChecksum")
    try {
      FSUtils.writeStringToFile(file, content)
    } finally {
      fileSystem.setWriteChecksum(writeChecksum)
      fileSystem.setVerifyChecksum(verifyChecksum)
    }
  }

  final private def tryReadFileSystemFlag(fileSystem: FileSystem, name: String) : Boolean = {
    if (fileSystem.isInstanceOf[ChecksumFileSystem]) {
      val field = classOf[ChecksumFileSystem].getDeclaredField(name)
      field.setAccessible(true)
      field.getBoolean(fileSystem)
    } else {
      true
    }
  }

    final private def databricksHack(path: String) : String = {
      if (path.startsWith("dbfs:/")) {
        "/dbfs/" + path.substring("dbfs:/".length)
      } else {
        path
      }
    }
}