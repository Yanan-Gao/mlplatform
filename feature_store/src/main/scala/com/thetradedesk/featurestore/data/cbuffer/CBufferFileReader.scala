package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.constants.FeatureConstants.{BytesToKeepAddressInChunk, BytesToKeepAddressInRecord}
import com.thetradedesk.featurestore.data.cbuffer.CBufferFileReader.{EFMAGIC, MAGIC, readFully, readNextInt}
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.{byteWidthOfArray, fillArray}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.slf4j.LoggerFactory

import java.io.{Closeable, EOFException, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * magic
 * total record count
 * chunk 1, record count, data size, data
 * chunk 2, record count, data size, data
 * total record count
 * magic
 */
// todo optimization
case class CBufferFileReader(path: Path, features: Array[CBufferFeature], options: CBufferOptions) extends Closeable {
  private var fileLen: Long = _
  private var f: FSDataInputStream = _
  private var totalRecordCount: Int = 0
  private var chunkRecordCount: Int = 0
  private var recordAddressTable: Array[Int] = new Array[Int](options.maxChunkRecordCount)
  private var recordOffset: Int = 0
  private var chunkBuffer: ByteBuffer = allocateBuffer(options.defaultChunkRecordSize * options.maxChunkRecordCount, options.useOffHeap, options.bigEndian)

  private var encryptedMode: Boolean = false

  private val LOG = LoggerFactory.getLogger(classOf[CBufferFileReader])
  private implicit val byteOrder: ByteOrder = if (options.bigEndian) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN

  def openFile(conf: Configuration): Int = {
    val fs = path.getFileSystem(conf)
    val stat = fs.getFileStatus(path)
    this.fileLen = stat.getLen
    this.f = fs.open(stat.getPath)
    readTotalRecordCount()
    this.totalRecordCount
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

    if (compareMagic(MAGIC, this.chunkBuffer, RECORD_COUNT_LENGTH)) encryptedMode = false
    else if (compareMagic(EFMAGIC, this.chunkBuffer, RECORD_COUNT_LENGTH)) encryptedMode = true
    else throw new RuntimeException(filePath + " is not a CBuffer file.")

    this.totalRecordCount = chunkBuffer.getInt

    this.f.seek(0)
    this.f.readFully(this.chunkBuffer.array(), 0, MAGIC.length)

    if ((encryptedMode && !compareMagic(EFMAGIC, this.chunkBuffer)) || (!encryptedMode && !compareMagic(MAGIC, this.chunkBuffer)))
      throw new RuntimeException(filePath + " magic head and tail are inconsistent.")
  }

  private def compareMagic(magic: Array[Byte], bf: ByteBuffer, offset: Int = 0): Boolean = {
    for (i <- magic.indices) {
      if (bf.get(offset + i) != magic(i)) return false
    }
    true
  }

  def readBatch(num: Int, columnVectors: Array[WritableColumnVector]) = {
    for (i <- features.indices) {
      for (j <- 0 until num) {
        readRow(features(i), j, columnVectors(i))
      }
    }
    this.recordOffset += num
  }

  // todo optimization
  private def readRow(feature: CBufferFeature, rowId: Int, columnVector: WritableColumnVector): Unit = {
    val rowStart = this.recordAddressTable(this.recordOffset + rowId)
    val index = rowStart + feature.index
    // single boolean
    if (!feature.isArray && feature.dataType == DataType.Bool) {
      columnVector.putBoolean(rowId, (this.chunkBuffer.get(index) & (1 << feature.offset)) != 0)
      return
    }

    // single numerical value
    if (!feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double) {
      readValue(columnVector, rowId, feature.dataType, index)
      return
    }

    val start = if (feature.offset > 1) index // fixed length array
    else rowStart + this.chunkBuffer.getShort(index).intValue() // var length feature

    val length = if (feature.offset > 1) byteWidthOfArray(feature.dataType, feature.offset) // fixed length array
    else rowStart + this.chunkBuffer.getShort(index + BytesToKeepAddressInRecord) - start // non-last var length feature

    // string
    if (!feature.isArray && feature.dataType == DataType.String) {
      columnVector.putByteArray(rowId, this.chunkBuffer.array(), start, length)
      return
    }

    // array of numerical values
    if (feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double) {
      readArrayValue(columnVector, rowId, feature.dataType, start, length)
      return
    }

    throw new UnsupportedOperationException(s"type ${feature.dataType} isArray ${feature.isArray}  is not supported")
  }

  // todo optimization
  def readArrayValue(columnVector: WritableColumnVector, rowId: Int, dataType: DataType, index: Int, byteLength: Int): Unit = {
    dataType match {
      case DataType.Byte =>
        columnVector.putByteArray(rowId, this.chunkBuffer.array(), index, byteLength)
      case DataType.Short =>
        val shortArray = fillArray[Short](this.chunkBuffer, dataType, index, byteLength)
        val result = columnVector.arrayData().appendShorts(shortArray.length, shortArray, 0)
        columnVector.putArray(rowId, result, shortArray.length)
      case DataType.Int =>
        val intArray = fillArray[Int](this.chunkBuffer, dataType, index, byteLength)
        val result = columnVector.arrayData().appendInts(intArray.length, intArray, 0)
        columnVector.putArray(rowId, result, intArray.length)
      case DataType.Long =>
        val longArray = fillArray[Long](this.chunkBuffer, dataType, index, byteLength)
        val result = columnVector.arrayData().appendLongs(longArray.length, longArray, 0)
        columnVector.putArray(rowId, result, longArray.length)
      case DataType.Float =>
        val floatArray = fillArray[Float](this.chunkBuffer, dataType, index, byteLength)
        val result = columnVector.arrayData().appendFloats(floatArray.length, floatArray, 0)
        columnVector.putArray(rowId, result, floatArray.length)
      case DataType.Double =>
        val doubleArray = fillArray[Double](this.chunkBuffer, dataType, index, byteLength)
        val result = columnVector.arrayData().appendDoubles(doubleArray.length, doubleArray, 0)
        columnVector.putArray(rowId, result, doubleArray.length)
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  def readValue(columnVector: WritableColumnVector, rowId: Int, dataType: DataType, index: Int): Unit = {
    dataType match {
      case DataType.Byte => columnVector.putByte(rowId, this.chunkBuffer.get(index))
      case DataType.Short => columnVector.putShort(rowId, this.chunkBuffer.getShort(index))
      case DataType.Int => columnVector.putInt(rowId, this.chunkBuffer.getInt(index))
      case DataType.Long => columnVector.putLong(rowId, this.chunkBuffer.getLong(index))
      case DataType.Float => columnVector.putFloat(rowId, this.chunkBuffer.getFloat(index))
      case DataType.Double => columnVector.putDouble(rowId, this.chunkBuffer.getDouble(index))
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  // load whole chunk into bytebuffer, need to optimize it to reduce data copy
  def prepareNextChunk(): Int = {
    this.chunkRecordCount = readNextInt(this.f)
    val dataSize = readNextInt(this.f)

    val leastBufferSize = math.max(dataSize, this.chunkRecordCount * BytesToKeepAddressInChunk)
    // renew byte buffer, todo -> add decompression codec here
    if (leastBufferSize > this.chunkBuffer.capacity()) {
      this.chunkBuffer = allocateBuffer(nextAllocateSize(leastBufferSize), options.useOffHeap, options.bigEndian)
    }

    // load chunk address table to buffer,
    this.chunkBuffer.clear()
    readFully(this.f, this.chunkBuffer.array(), 0, this.chunkRecordCount * BytesToKeepAddressInChunk)

    // renew array buffer
    if (this.chunkRecordCount >= this.recordAddressTable.length) {
      this.recordAddressTable = new Array[Int](nextAllocateSize(this.chunkRecordCount))
    }

    // update record offset
    for (i <- 0 until this.chunkRecordCount) {
      this.recordAddressTable.update(i, this.chunkBuffer.getInt(i << 2))
    }

    // load chunk data to buffer,
    this.chunkBuffer.clear()
    readFully(this.f, this.chunkBuffer.array(), 0, dataSize)

    this.recordOffset = 0

    this.chunkRecordCount
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
}

object CBufferFileReader {
  val MAGIC_STR: String = "CBU1"
  val MAGIC: Array[Byte] = MAGIC_STR.getBytes(StandardCharsets.US_ASCII)
  val EF_MAGIC_STR = "CBUE"
  val EFMAGIC: Array[Byte] = EF_MAGIC_STR.getBytes(StandardCharsets.US_ASCII)
  val INT_BYTES_LENGTH = 4

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
