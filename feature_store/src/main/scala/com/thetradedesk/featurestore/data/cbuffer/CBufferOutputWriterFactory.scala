package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.FileExtension
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

case class CBufferOutputWriterFactory(sparkSession: SparkSession, options: CBufferOptions, dataSchema: StructType) extends OutputWriterFactory {
  private val features = SchemaHelper.inferFeature(dataSchema, options.columnBased)
  CBufferDataSource.writeSchema(this.features, options)(sparkSession)

  // todo support compression codec
  override def getFileExtension(context: TaskAttemptContext): String = FileExtension

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    new CBufferOutputWriter(path, context, dataSchema, this.features, options)
  }
}