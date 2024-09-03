package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.constants.FeatureConstants.{BytesToKeepAddressInChunk, BytesToKeepAddressInRecord}
import com.thetradedesk.featurestore.data.cbuffer.CBufferChunk._
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.{byteWidthOfArray, byteWidthOfType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

case class CBufferChunk(schema: StructType, features: Array[CBufferFeature], options: CBufferOptions) {
  var size: Int = 0
  private val sizeBuffer: ByteBuffer = allocateBuffer(ChunkDataOffset, options.useOffHeap, options.bigEndian)
  private var chunkBuffer: ByteBuffer = allocateBuffer(options.defaultChunkRecordSize * options.maxChunkRecordCount, options.useOffHeap, options.bigEndian)
  private val ordinals = featureOrdinals()
  private val boolFeatureCount: Int = this.features.count(e => e.dataType == DataType.Bool)
  private val boolFeatureByteSize: Int = (boolFeatureCount + 7) >> 3
  private val lastFeature : CBufferFeature = this.features.last
  private val existVarLengthFeature: Boolean = lastFeature.dataType == DataType.String || (lastFeature.isArray && lastFeature.offset == 0)
  private val varLengthFeatureOffset: Int = lastFeature.index + BytesToKeepAddressInRecord + BytesToKeepAddressInRecord
  private val estimateRecordSize: Int = if (existVarLengthFeature) varLengthFeatureOffset else lastFeature.index + byteWidthOfArray(lastFeature.dataType, lastFeature.offset)
  private val varLengthFeatureStartOffset: Int = this.features.filter(e => e.dataType == DataType.String || (e.isArray && e.offset == 0)).map(_.index).headOption.getOrElse(0)
  private val addressTableBuffer: ByteBuffer = allocateBuffer(options.maxChunkRecordCount * BytesToKeepAddressInChunk, options.useOffHeap, options.bigEndian)
  private var start: Int = _
  private var refPosition: Int = _
  private var offStart: Int = _
  //  private var refStart: Int = _

  // todo optimize
  def write(value: InternalRow): Unit = {
    this.start = this.chunkBuffer.position()
    this.refPosition = this.start + this.varLengthFeatureStartOffset
    this.offStart = this.start + this.varLengthFeatureOffset
    this.addressTableBuffer.putInt(this.size * BytesToKeepAddressInChunk, this.start)

    // pre check
    checkBufferBound(this.start + estimateRecordSize)
    for (_ <- 0 until this.boolFeatureByteSize) {
      this.chunkBuffer.put(0.byteValue())
    }

    ordinals.foreach {
      case (CBufferFeature(_, index, offset, DataType.Bool, false), ordinal) =>
        if (value.getBoolean(ordinal)) {
          this.chunkBuffer.put(this.start + index, (this.chunkBuffer.get(this.start + index) | (1 << offset)).toByte)
        }
      case (CBufferFeature(_, _, _, DataType.Byte, false), ordinal) =>
        this.chunkBuffer.put(value.getByte(ordinal))
      case (CBufferFeature(_, _, _, DataType.Short, false), ordinal) =>
        this.chunkBuffer.putShort(value.getShort(ordinal))
      case (CBufferFeature(_, _, _, DataType.Int, false), ordinal) =>
        this.chunkBuffer.putInt(value.getInt(ordinal))
      case (CBufferFeature(_, _, _, DataType.Long, false), ordinal) =>
        this.chunkBuffer.putLong(value.getLong(ordinal))
      case (CBufferFeature(_, _, _, DataType.Float, false), ordinal) =>
        this.chunkBuffer.putFloat(value.getFloat(ordinal))
      case (CBufferFeature(_, _, _, DataType.Double, false), ordinal) =>
        this.chunkBuffer.putDouble(value.getDouble(ordinal))
      case (CBufferFeature(_, _, 0, DataType.Byte, true), ordinal) =>
        updateOffset()
        val bytes = value.getBinary(ordinal)
        checkBufferBound(this.offStart + bytes.length)
        this.chunkBuffer.put(bytes)
        resetOffset()
      case (CBufferFeature(_, _, 0, DataType.Short, true), ordinal) =>
        writeVarLengthArray(value, DataType.Short, ordinal, (arr, i) =>
          this.chunkBuffer.putShort(arr.getShort(i)))
      case (CBufferFeature(_, _, 0, DataType.Int, true), ordinal) =>
        writeVarLengthArray(value, DataType.Int, ordinal, (arr, i) =>
          this.chunkBuffer.putInt(arr.getInt(i)))
      case (CBufferFeature(_, _, 0, DataType.Long, true), ordinal) =>
        writeVarLengthArray(value, DataType.Long, ordinal, (arr, i) =>
          this.chunkBuffer.putLong(arr.getLong(i)))
      case (CBufferFeature(_, _, 0, DataType.Float, true), ordinal) =>
        writeVarLengthArray(value, DataType.Float, ordinal, (arr, i) =>
          this.chunkBuffer.putFloat(arr.getFloat(i)))
      case (CBufferFeature(_, _, 0, DataType.Double, true), ordinal) =>
        writeVarLengthArray(value, DataType.Double, ordinal, (arr, i) =>
          this.chunkBuffer.putDouble(arr.getDouble(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Byte, true), ordinal) =>
        val bytes = value.getBinary(ordinal)
        assert(arrayLength == bytes.length, "fixed array feature length must be equal as defined")
        this.chunkBuffer.put(bytes)
      case (CBufferFeature(_, _, arrayLength, DataType.Short, true), ordinal) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.chunkBuffer.putShort(arr.getShort(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Int, true), ordinal) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.chunkBuffer.putInt(arr.getInt(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Long, true), ordinal) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.chunkBuffer.putLong(arr.getLong(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Float, true), ordinal) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.chunkBuffer.putFloat(arr.getFloat(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Double, true), ordinal) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.chunkBuffer.putDouble(arr.getDouble(i)))
      case (CBufferFeature(_, _, _, DataType.String, false), ordinal) =>
        updateOffset()
        val bytes = value.getUTF8String(ordinal).getBytes
        checkBufferBound(this.offStart + bytes.length)
        this.chunkBuffer.put(bytes)
        resetOffset()
      case _ => throw new UnsupportedOperationException(s"this should not happen")
    }

    if (existVarLengthFeature) {
      updateOffset()
    }
    this.size += 1
  }

  private def updateOffset(): Unit = {
    this.chunkBuffer.position(this.refPosition)
    this.chunkBuffer.putShort((this.offStart - this.start).toShort)
    this.chunkBuffer.position(this.offStart)
  }

  private def resetOffset(): Unit = {
    this.refPosition += BytesToKeepAddressInRecord
    this.offStart = this.chunkBuffer.position()
  }

  private def writeFixedLengthArray(value: InternalRow, arrayLength: Int, ordinal: Int, op: (ArrayData, Int) => Unit): Unit = {
    val arr = value.getArray(ordinal)
    assert(arrayLength == arr.numElements(), "fixed array feature length must be equal as defined")
    for (i <- 0 until arrayLength) {
      op(arr, i)
    }
  }

  // todo fix the bug with address table
  private def checkBufferBound(offset: Int) = {
    if (offset > this.chunkBuffer.capacity()) {
      if (options.fixedChunkBuffer) {
        throw new UnsupportedOperationException(s"current chunk size is out of capacity ${this.chunkBuffer.capacity()}")
      }
      println(s"chunk buffer extended original size ${this.chunkBuffer.capacity()} expectation ${offset}")
      val newChunkBuffer = allocateBuffer(nextAllocateSize(offset), this.options.useOffHeap, this.options.bigEndian)
      val position = this.chunkBuffer.position()
      this.chunkBuffer.rewind()
      newChunkBuffer.put(this.chunkBuffer)
      newChunkBuffer.position(position)
      ByteBufferUtil.clean(this.chunkBuffer)
      this.chunkBuffer = newChunkBuffer
    }
  }

  private def writeVarLengthArray(value: InternalRow, dataType: DataType, ordinal: Int, op: (ArrayData, Int) => Unit): Unit = {
    updateOffset()
    val arr = value.getArray(ordinal)
    val size = if (arr == null) 0 else arr.numElements()
    if (size != 0) {
      checkBufferBound(this.offStart + size * byteWidthOfType(dataType))
      for (i <- 0 until size) {
        op(arr, i)
      }
    }
    resetOffset()
  }

  def reset(): Unit = {
    this.size = 0
    this.chunkBuffer.position(DataStart)
  }

  def flush(outputStream: OutputStream) = {
    val totalChunkSize = ChunkDataOffset + this.size * BytesToKeepAddressInChunk + this.chunkBuffer.position()
    this.sizeBuffer.putInt(DataStart, this.size)
    this.sizeBuffer.putInt(ChunkDataSizeOffset, this.chunkBuffer.position())
    outputStream.write(this.sizeBuffer.array())
    outputStream.write(this.addressTableBuffer.array(), DataStart, this.size * BytesToKeepAddressInChunk)
    // flush data to output stream
    outputStream.write(this.chunkBuffer.array(), DataStart, this.chunkBuffer.position())

    this.reset()
    totalChunkSize
  }

  private def featureOrdinals(): Array[(CBufferFeature, Int)] = {
    val fieldToOrd = this.schema
      .fields
      .map(_.name)
      .zipWithIndex
      .toMap

    this.features
      .map(e => (e, fieldToOrd(e.name)))
  }
}

object CBufferChunk {
  private val DataStart = 0
  private val ChunkDataSizeOffset = 4
  private val ChunkDataOffset = 8
}
