package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.{COLUMN_MAGIC, ChunkDataOffset, EFMAGIC, INT_BYTES_LENGTH, MAGIC}
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.allocateBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.slf4j.LoggerFactory

import java.io.{Closeable, EOFException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}

class CBufferFileReaderFacade(path: Path, features: Array[CBufferFeature], options: CBufferOptions) extends Closeable with CBufferFileReader {
  protected var fileLen: Long = _
  protected var f: FSDataInputStream = _
  protected var totalRecordCount: Int = 0

  protected var reader: CBufferFileReader = _

  protected var chunkBuffer: ByteBuffer = allocateBuffer(ChunkDataOffset, options.useOffHeap, options.bigEndian)

  protected var encryptedMode: Boolean = false
  private var columnBasedMode: Boolean = false

  protected val LOG = LoggerFactory.getLogger(classOf[CBufferFileReaderFacade])

  def openFile(conf: Configuration): Int = {
    val fs = path.getFileSystem(conf)
    val stat = fs.getFileStatus(path)
    this.fileLen = stat.getLen
    this.f = fs.open(stat.getPath)
    readTotalRecordCount()
    this.reader = initCBufferReader()
    this.totalRecordCount
  }

  private def initCBufferReader(): CBufferFileReader = {
    if (columnBasedMode) {
      CBufferColumnBasedFileReader(this.f, features, options)
    } else {
      CBufferRowBasedFileReader(this.f, features, options)
    }
  }

  private def readTotalRecordCount(): Unit = {
    val filePath = path.toString
    LOG.debug("File {} length {}", filePath, this.fileLen)

    val RECORD_COUNT_LENGTH = 4
    if (this.fileLen < MAGIC.length + RECORD_COUNT_LENGTH + MAGIC.length) { // MAGIC + record count + data + MAGIC
      throw new RuntimeException(filePath + " is not a CBuffer file (length is too low: " + this.fileLen + ")")
    }

    // Read record length and magic string - with a single seek
    this.f.seek(this.fileLen - MAGIC.length - RECORD_COUNT_LENGTH)
    this.f.read(chunkBuffer.array(), 0, MAGIC.length + RECORD_COUNT_LENGTH)

    if (compareMagic(COLUMN_MAGIC, this.chunkBuffer, RECORD_COUNT_LENGTH)) {
      encryptedMode = false
      columnBasedMode = true
    }
    else if (compareMagic(MAGIC, this.chunkBuffer, RECORD_COUNT_LENGTH)) {
      encryptedMode = false
      columnBasedMode = false
    }
    else if (compareMagic(EFMAGIC, this.chunkBuffer, RECORD_COUNT_LENGTH)) {
      encryptedMode = true
      columnBasedMode = false
    }
    else throw new RuntimeException(filePath + " is not a CBuffer file.")

    this.totalRecordCount = chunkBuffer.getInt

    this.f.seek(0)
    this.f.readFully(this.chunkBuffer.array(), 0, MAGIC.length)

    if ((encryptedMode && !compareMagic(EFMAGIC, this.chunkBuffer))
      || (!encryptedMode && !columnBasedMode && !compareMagic(MAGIC, this.chunkBuffer))
      || (!encryptedMode && columnBasedMode && !compareMagic(COLUMN_MAGIC, this.chunkBuffer)))
      throw new RuntimeException(filePath + " magic head and tail are inconsistent.")
  }

  private def compareMagic(magic: Array[Byte], bf: ByteBuffer, offset: Int = 0): Boolean = {
    for (i <- magic.indices) {
      if (bf.get(offset + i) != magic(i)) return false
    }
    true
  }

  override def close(): Unit = {
    try if (f != null) {
      f.close()
    }
    finally {
      if (options.codecFactory != null) {
        options.codecFactory.release()
      }
    }
  }

  override def readBatch(num: Int, columnVectors: Array[WritableColumnVector]): Unit = {
    this.reader.readBatch(num, columnVectors)
  }

  override def prepareNextChunk(): Int = {
    this.reader.prepareNextChunk()
  }
}

object CBufferFileReaderFacade {
  def readNextInt(f: InputStream)(implicit byteOrder: ByteOrder): Int = {
    val bytes = new Array[Byte](INT_BYTES_LENGTH)
    readFully(f, bytes, 0, INT_BYTES_LENGTH)

    val buffer = ByteBuffer.wrap(bytes).order(byteOrder)
    buffer.getInt(0)
  }

  def readFully(f: InputStream, bytes: Array[Byte]): Unit = {
    readFully(f, bytes, 0, bytes.length)
  }

  def readFully(f: InputStream, bytes: Array[Byte], start: Int, len: Int): Unit = {
    var offset = start
    var remaining = len
    while (remaining > 0) {
      val bytesRead = f.read(bytes, offset, remaining)
      if (bytesRead < 0) throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
      remaining -= bytesRead
      offset += bytesRead
    }
  }
}
