package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.{DefaultSchemaFileName, ShortName}
import com.thetradedesk.featurestore.utils.{FileHelper, PathUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import upickle.default.{read, write}

class CBufferDataSource extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[CBufferFileFormat]

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    CBufferTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    CBufferTable(
      tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }

  override def shortName(): String = ShortName
}

object CBufferDataSource {
  final def readFeatureSchema(
                               parsedOptions: CBufferOptions,
                               paths: Seq[String])
                             (implicit sparkSession: SparkSession): Array[CBufferFeature] = {
    if (parsedOptions.schemaPath.nonEmpty) {
      tryReadSchema(parsedOptions.schemaPath.get)(sparkSession).get
    } else {
      for (path <- paths) {
        val schemaPath = PathUtils.concatPath(path, DefaultSchemaFileName)
        if (FileHelper.fileExists(schemaPath)) {
          val featureOption = tryReadSchema(schemaPath)
          if (featureOption.nonEmpty) {
            return featureOption.get
          }
        } else if (path.endsWith(".cb")) {
          val schemaPathOption = PathUtils.truncateToParentDirectory(path)
          if (schemaPathOption.nonEmpty) {
            val featureOption = tryReadSchema(PathUtils.concatPath(schemaPathOption.get, DefaultSchemaFileName))
            if (featureOption.nonEmpty) {
              return featureOption.get
            }
          }
        }
      }
      throw new IllegalStateException("Schema Path must be informed in options for CBuffer format")
    }
  }

  private def tryReadSchema(path: String)(implicit sparkSession: SparkSession): Option[Array[CBufferFeature]] = {
    try {
      val json = FileHelper.readStringFromFile(path)(sparkSession)
      println("cbuffer schema json: " + path + ", result: " + json)
      Some(read[Array[CBufferFeature]](json))
    } catch {
      case _: Exception => None
    }
  }

  final def inferSchema(
                         options: CBufferOptions,
                         paths: Seq[String])
                       (implicit sparkSession: SparkSession): Option[StructType] = {
    val featureOption = readFeatureSchema(options, paths)
    Some(SchemaHelper.inferSchema(featureOption))
  }

  final def writeSchema(
                         features: Array[CBufferFeature],
                         options: CBufferOptions,
                         overrideSchema: Boolean = true)
                       (implicit sparkSession: SparkSession) = {
    val featureSchemaJson = write(features)
    val schemaPath = if (options.schemaPath.nonEmpty) options.schemaPath.get else PathUtils.concatPath(options.outputPath.get, DefaultSchemaFileName)

    if (overrideSchema) {
      println("cbuffer schema: " + featureSchemaJson)
      FileHelper.deleteFile(schemaPath)(sparkSession)
      FileHelper.writeStringToFile(schemaPath, featureSchemaJson)(sparkSession)
    } else if (!FileHelper.fileExists(schemaPath)(sparkSession)) {
      FileHelper.writeStringToFile(schemaPath, featureSchemaJson)(sparkSession)
    }
  }
}