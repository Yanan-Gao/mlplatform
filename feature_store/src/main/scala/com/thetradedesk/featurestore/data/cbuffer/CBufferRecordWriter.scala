package com.thetradedesk.featurestore.data.cbuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

case class CBufferRecordWriter(path: Path, context: TaskAttemptContext, dataSchema: StructType, features: Array[CBufferFeature], options: CBufferOptions) extends RecordWriter[Void, InternalRow] {
  private val writer: CBufferFileWriter = CBufferFileWriter(path, context, dataSchema, features, options)

  override def write(key: Void, value: InternalRow): Unit = this.writer.write(value)

  override def close(taskAttemptContext: TaskAttemptContext): Unit = this.writer.close()
}
