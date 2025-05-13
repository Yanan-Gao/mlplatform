package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.constants.FeatureConstants.{BytesToKeepAddressInChunk, BytesToKeepAddressInRecord}
import com.thetradedesk.featurestore.data.cbuffer.CBufferFileReaderFacade.{readFully, readNextInt}
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.{byteWidthOfArray, fillArray}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

/**
 * magic
 * total record count
 * chunk 1, record count, data size, data
 * chunk 2, record count, data size, data
 * total record count
 * magic
 */
// todo optimization
case class CBufferRowBasedFileReader(f: FSDataInputStream, features: Array[CBufferFeature], options: CBufferOptions) extends AbstractCBufferFileReader(options: CBufferOptions) {
  private var recordAddressTable: Array[Int] = new Array[Int](options.maxChunkRecordCount)
  private var recordOffset: Int = 0

  def readBatch(num: Int, columnVectors: Array[WritableColumnVector]): Unit = {
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
  private def readArrayValue(columnVector: WritableColumnVector, rowId: Int, dataType: DataType, index: Int, byteLength: Int): Unit = {
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

  private def readValue(columnVector: WritableColumnVector, rowId: Int, dataType: DataType, index: Int): Unit = {
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
}
