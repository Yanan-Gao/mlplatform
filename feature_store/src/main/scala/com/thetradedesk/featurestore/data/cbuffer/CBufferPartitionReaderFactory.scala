package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, RecordReader, TaskAttemptID, TaskID, TaskType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import upickle.default.read

import java.net.URI
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

case class CBufferPartitionReaderFactory(
                                          sqlConf: SQLConf,
                                          broadcastedConf: Broadcast[SerializableConfiguration],
                                          dataSchema: StructType,
                                          readDataSchema: StructType,
                                          partitionSchema: StructType,
                                          options: CBufferOptions,
                                          filters: Array[Filter]) extends FilePartitionReaderFactory with Logging {
  private val readColumns = readDataSchema.fields.map(_.name).toSet
  private val features = SchemaHelper.inferFeature(dataSchema)
    .filter(e => readColumns.contains(e.name))

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val fileReader = if (file.filePath.endsWith(".cb")) {
      val reader = createFileReader(file)

      new PartitionReader[InternalRow] {
        override def next(): Boolean = reader.nextKeyValue()

        override def get(): InternalRow = reader.getCurrentValue

        override def close(): Unit = reader.close()
      }
    } else {
      new PartitionReader[InternalRow] {
        override def next(): Boolean = false

        override def get(): InternalRow = null

        override def close(): Unit = {}
      }
    }

    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  private def createFileReader(file: PartitionedFile) : CBufferRecordReader = {
    val recordReader = new CBufferRecordReader(options, readDataSchema, features, options.defaultReachBatch)

    val filePath = new Path(new URI(file.filePath))
    val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

    val conf = broadcastedConf.value.value
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    recordReader.initialize(split, hadoopAttemptContext)
    recordReader.initBatch(partitionSchema, file.partitionValues)

    recordReader
  }
}
