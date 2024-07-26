package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

class CBufferOutputWriter(val path: String, context: TaskAttemptContext, dataSchema: StructType, features: Array[CBufferFeature], options: CBufferOptions) extends OutputWriter {
  private val recordWriter: RecordWriter[Void, InternalRow] = CBufferRecordWriter(new Path(path), context, dataSchema, features, options)

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}
