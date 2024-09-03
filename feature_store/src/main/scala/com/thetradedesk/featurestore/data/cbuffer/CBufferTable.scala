package com.thetradedesk.featurestore.data.cbuffer

import scala.jdk.CollectionConverters._
import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.ShortName
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class CBufferTable(
                         name: String,
                         sparkSession: SparkSession,
                         options: CaseInsensitiveStringMap,
                         paths: Seq[String],
                         userSpecifiedSchema: Option[StructType],
                         fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {
    override def build(): Write = CBufferWrite(sparkSession, paths, formatName, supportsDataType, info)
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    CBufferDataSource.inferSchema(CBufferOptions(options.asScala.toMap), paths)(sparkSession)
  }

  override def formatName: String = ShortName

  override def supportsDataType(dataType: DataType): Boolean = supportsDataType(dataType, topLevel = true)

  private def supportsDataType(dataType: DataType, topLevel: Boolean): Boolean = dataType match {
    case _: BinaryType => topLevel
    case _: BooleanType => topLevel
    case _: ByteType => true
    case _: ShortType => true
    case _: IntegerType => true
    case _: LongType => true
    case _: FloatType => true
    case _: DoubleType => true
    case _: StringType => topLevel
    case ArrayType(elementType, _) => topLevel && supportsDataType(elementType, topLevel = false)
    case _ => false
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    CBufferScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }
}
