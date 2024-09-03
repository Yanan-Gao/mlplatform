package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.BitsInInteger
import com.thetradedesk.featurestore.data.cbuffer.CBufferFileReader.{EFMAGIC, MAGIC}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.io.{Closeable, OutputStream}
import java.nio.ByteBuffer

/**
 * magic
 * chunk 1, record count, data size, address table(offset from current position, include one extra offset to end of the data), data
 * chunk 2, record count, data size, address table, data
 * total record count
 * magic
 */

// todo optimization
case class CBufferFileWriter(path: Path, context: TaskAttemptContext, schema: StructType, features: Array[CBufferFeature], options: CBufferOptions) extends Closeable {
  private val LOG: Logger = LoggerFactory.getLogger(CBufferFileWriter.getClass)
  private var recordCount: Int = 0
  private var dataSize: Long = 0
  private val intBuffer: ByteBuffer = MemoryHelper.allocateBuffer(BitsInInteger, options.useOffHeap, options.bigEndian)
  private var chunk: CBufferChunk = CBufferChunk(schema, features, options)
  private val fs = path.getFileSystem(context.getConfiguration)
  private val outputStream: OutputStream = fs.create(path, false)
  private var closed: Boolean = false
  init()

  private def init(): Unit = {
    writeMagic()
  }

  // check and write chunk
  private def checkChunkSizeReached(): Unit = {
    if (options.maxChunkRecordCount == chunk.size) {
      this.flushChunk()
    }
  }

  def write(value: InternalRow) = {
    this.chunk.write(value)
    recordCount += 1
    checkChunkSizeReached()
  }

  // todo support compression codec
  private def flushChunk() = {
    val chunkSize = this.chunk.flush(this.outputStream) // write data
    this.dataSize += chunkSize
    checkFileSize()
  }

  private def writeRecordCount(): Unit = {
    this.intBuffer.putInt(0, this.recordCount)
    this.outputStream.write(intBuffer.array())
    this.dataSize += BitsInInteger
  }

  private def writeMagic(): Unit = {
    val magic = if (options.encryptedMode) EFMAGIC else MAGIC
    this.outputStream.write(magic)
    this.dataSize += magic.length
  }

  private def complete(): Unit = {
    this.flushChunk()
    this.writeRecordCount()
    this.writeMagic()
    this.checkFileSize()
    this.flush()
  }

  private def flush(): Unit = {
    if (this.outputStream != null) {
      this.outputStream.flush()
    }
  }

  private def checkFileSize(): Unit = {
    if (this.dataSize > options.maxFileSize) {
      throw new IllegalStateException(s"current output file size ${this.dataSize} is larger than maximal file size ${options.maxFileSize}, you may set maximal file size with `maxFileSize` in options")
    }
  }

  override def close(): Unit = {
    if (!closed) {
      this.complete()

      if (this.outputStream != null) {
        this.outputStream.close()
      }

      this.chunk = null
      this.closed = true
    }
  }
}
