package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

case class CBufferWrite(sparkSession: SparkSession,
                        paths: Seq[String],
                        formatName: String,
                        supportsDataType: DataType => Boolean,
                        info: LogicalWriteInfo) extends FileWrite {

  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val cBufferOptions = CBufferOptions(options)
    CBufferOutputWriterFactory(sparkSession, cBufferOptions, dataSchema)
  }
}
