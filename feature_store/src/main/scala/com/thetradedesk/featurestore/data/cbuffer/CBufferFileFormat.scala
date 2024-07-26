package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.{FileExtension, ShortName}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType

class CBufferFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable{

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    val paths = files.map(e => if (e.isDirectory) e.getPath.toUri.getPath else e.getPath.getParent.toUri.getPath).distinct
    CBufferDataSource.inferSchema(sparkSession, CBufferOptions(options), paths)
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val cBufferOptions = CBufferOptions(options)
    CBufferOutputWriterFactory(sparkSession, cBufferOptions, dataSchema)
  }

  override def shortName(): String = ShortName

  override def toString: String = ShortName
}
