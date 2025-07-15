package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.constants.FeatureConstants.BytesToKeepAddressInChunk
import com.thetradedesk.featurestore.data.cbuffer.CBufferFileReaderFacade.{readFully, readNextInt}
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.{byteWidthOfArray, byteWidthOfType, fillArray, lengthOfType, readValue}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

import java.nio.ByteBuffer
import scala.reflect.ClassTag

case class CBufferColumnBasedFileReader(f: FSDataInputStream, features: Array[CBufferFeature], options: CBufferOptions) extends AbstractCBufferFileReader(options: CBufferOptions) {
  private val columnAddressTable: Array[Int] = new Array[Int](features.length)
  private var recordOffset: Int = 0

  override def readBatch(num: Int, columnVectors: Array[WritableColumnVector]): Unit = {
    for (i <- this.features.indices) {
      readRow(i, num, columnVectors(i))
    }
    this.recordOffset += num
  }

  private def readRow(featureIndex: Int, num: Int, columnVector: WritableColumnVector): Unit = {
    val columnStart = this.columnAddressTable(featureIndex)
    val feature = this.features(featureIndex)
    // single boolean
    if (!feature.isArray && feature.dataType == DataType.Bool) {
      for (rowId <- 0 until num) {
        val currentOffset = this.recordOffset + rowId
        columnVector.putBoolean(rowId, (this.chunkBuffer.get(columnStart + (currentOffset >> 3)) & (1 << (currentOffset & 7))) != 0)
      }
      return
    }

    // single numerical value
    if (!feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double) {
      val index = columnStart + this.recordOffset * byteWidthOfType(feature.dataType)
      readSingleValue(columnVector, num, feature.dataType, index)
      return
    }

    // fixed array
    if (feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double && feature.offset != 0) {
      val byteLength = byteWidthOfArray(feature.dataType, feature.offset, varLength = false)
      val index = columnStart + this.recordOffset * byteLength
      readFixedArrayValue(columnVector, num, feature.dataType, index, byteLength)
      return
    }

    // var-length array
    if (feature.isArray && feature.dataType >= DataType.Byte && feature.dataType <= DataType.Double && feature.offset == 0) {
      val index = columnStart + this.recordOffset * BytesToKeepAddressInChunk
      readVarLengthArrayValue(columnVector, num, feature.dataType, index)
      return
    }

    // string
    if (!feature.isArray && feature.dataType == DataType.String) {
      var refEndOffset = columnStart + this.recordOffset * BytesToKeepAddressInChunk
      val refDataOffset = columnStart + this.chunkRecordCount * BytesToKeepAddressInChunk
      var startOffset = if (this.recordOffset == 0) 0 else this.chunkBuffer.getInt(refEndOffset - BytesToKeepAddressInChunk)
      for (rowId <- 0 until num) {
        val endOffset = this.chunkBuffer.getInt(refEndOffset)
        columnVector.putByteArray(rowId, this.chunkBuffer.array(), refDataOffset + startOffset, endOffset - startOffset)
        startOffset = endOffset
        refEndOffset = refEndOffset + BytesToKeepAddressInChunk
      }
      return
    }

    throw new UnsupportedOperationException(s"type ${feature.dataType} isArray ${feature.isArray}  is not supported")
  }

  private def readVarLengthArrayValue(columnVector: WritableColumnVector, num: Int, dataType: DataType, index: Int): Unit = {
    var refEndOffset = index + this.recordOffset * BytesToKeepAddressInChunk
    val refDataOffset = index + this.chunkRecordCount * BytesToKeepAddressInChunk
    var startOffset = if (this.recordOffset == 0) 0 else this.chunkBuffer.getInt(refEndOffset - BytesToKeepAddressInChunk)

    val repeatCopy = (op: (WritableColumnVector, Int) => Int) => {
      for (rowId <- 0 until num) {
        val endOffset = this.chunkBuffer.getInt(refEndOffset)
        val byteLength = endOffset - startOffset
        copyArray(columnVector, this.chunkBuffer, dataType, refDataOffset + startOffset, rowId, byteLength, op)
        startOffset = endOffset
        refEndOffset = refEndOffset + BytesToKeepAddressInChunk
      }
    }

    dataType match {
      case DataType.Byte =>
        for (rowId <- 0 until num) {
          val endOffset = this.chunkBuffer.getInt(refEndOffset)
          val byteLength = endOffset - startOffset
          columnVector.putByteArray(rowId, this.chunkBuffer.array(), refDataOffset + startOffset, byteLength)
          startOffset = endOffset
          refEndOffset = refEndOffset + BytesToKeepAddressInChunk
        }
      case DataType.Short => repeatCopy((columnVector, arrayLength) => columnVector.appendShorts(arrayLength, shortArrayCache, 0))
      case DataType.Int => repeatCopy((columnVector, arrayLength) => columnVector.appendInts(arrayLength, intArrayCache, 0))
      case DataType.Long => repeatCopy((columnVector, arrayLength) => columnVector.appendLongs(arrayLength, longArrayCache, 0))
      case DataType.Float => repeatCopy((columnVector, arrayLength) => columnVector.appendFloats(arrayLength, floatArrayCache, 0))
      case DataType.Double => repeatCopy((columnVector, arrayLength) => columnVector.appendDoubles(arrayLength, doubleArrayCache, 0))
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  private def readFixedArrayValue(columnVector: WritableColumnVector, num: Int, dataType: DataType, index: Int, byteLength: Int): Unit = {
    dataType match {
      case DataType.Byte =>
        for (rowId <- 0 until num)
          columnVector.putByteArray(rowId, this.chunkBuffer.array(), index + (byteLength * rowId), byteLength)
      case DataType.Short =>
        for (rowId <- 0 until num)
          copyArray(columnVector, this.chunkBuffer, dataType, index + (byteLength * rowId), rowId, byteLength, (columnVector, arrayLength) => columnVector.appendShorts(arrayLength, shortArrayCache, 0))
      case DataType.Int =>
        for (rowId <- 0 until num)
          copyArray(columnVector, this.chunkBuffer, dataType, index + (byteLength * rowId), rowId, byteLength, (columnVector, arrayLength) => columnVector.appendInts(arrayLength, intArrayCache, 0))
      case DataType.Long =>
        for (rowId <- 0 until num)
          copyArray(columnVector, this.chunkBuffer, dataType, index + (byteLength * rowId), rowId, byteLength, (columnVector, arrayLength) => columnVector.appendLongs(arrayLength, longArrayCache, 0))
      case DataType.Float =>
        for (rowId <- 0 until num)
          copyArray(columnVector, this.chunkBuffer, dataType, index + (byteLength * rowId), rowId, byteLength, (columnVector, arrayLength) => columnVector.appendFloats(arrayLength, floatArrayCache, 0))
      case DataType.Double =>
        for (rowId <- 0 until num)
          copyArray(columnVector, this.chunkBuffer, dataType, index + (byteLength * rowId), rowId, byteLength, (columnVector, arrayLength) => columnVector.appendDoubles(arrayLength, doubleArrayCache, 0))
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  private def readSingleValue(columnVector: WritableColumnVector, num: Int, dataType: DataType, index: Int): Unit = {
    dataType match {
      case DataType.Byte => for (rowId <- 0 until num) columnVector.putByte(rowId, this.chunkBuffer.get(index + rowId))
      case DataType.Short => for (rowId <- 0 until num) columnVector.putShort(rowId, this.chunkBuffer.getShort(index + (rowId << 1)))
      case DataType.Int => for (rowId <- 0 until num) columnVector.putInt(rowId, this.chunkBuffer.getInt(index + (rowId << 2)))
      case DataType.Long => for (rowId <- 0 until num) columnVector.putLong(rowId, this.chunkBuffer.getLong(index + (rowId << 3)))
      case DataType.Float => for (rowId <- 0 until num) columnVector.putFloat(rowId, this.chunkBuffer.getFloat(index + (rowId << 2)))
      case DataType.Double => for (rowId <- 0 until num) columnVector.putDouble(rowId, this.chunkBuffer.getDouble(index + (rowId << 3)))
      case _ => throw new UnsupportedOperationException(s"type ${dataType}  is not supported")
    }
  }

  // load whole chunk into bytebuffer, need to optimize it to reduce data copy
  def prepareNextChunk(): Int = {
    this.chunkRecordCount = readNextInt(this.f)
    val dataSize = readNextInt(this.f)

    val leastBufferSize = math.max(dataSize, this.features.length * BytesToKeepAddressInChunk)
    // renew byte buffer, todo -> add decompression codec here
    if (leastBufferSize > this.chunkBuffer.capacity()) {
      this.chunkBuffer = allocateBuffer(nextAllocateSize(leastBufferSize), options.useOffHeap, options.bigEndian)
    }

    // load chunk address table to buffer,
    this.chunkBuffer.clear()
    readFully(this.f, this.chunkBuffer.array(), 0, this.features.length * BytesToKeepAddressInChunk)

    // update record offset
    for (i <- this.features.indices) {
      this.columnAddressTable.update(i, this.chunkBuffer.getInt(i << 2))
    }

    // load chunk data to buffer,
    this.chunkBuffer.clear()
    readFully(this.f, this.chunkBuffer.array(), 0, dataSize)

    this.recordOffset = 0

    this.chunkRecordCount
  }
}
