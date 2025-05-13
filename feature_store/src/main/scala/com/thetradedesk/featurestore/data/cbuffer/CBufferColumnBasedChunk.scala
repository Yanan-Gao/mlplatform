package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.constants.FeatureConstants.BytesToKeepAddressInChunk
import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants._
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.byteWidthOfType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.nio.ByteBuffer

case class CBufferColumnBasedChunk(schema: StructType, features: Array[CBufferFeature], options: CBufferOptions) extends CBufferChunk(schema, features, options) {
  private val columnBuffers: Array[ByteBuffer] = initColumnBuffers()
  private val columnOffsetByteBuffer: ByteBuffer = allocateBuffer(features.length * BytesToKeepAddressInChunk, options.useOffHeap, options.bigEndian)
  private val varLengthColumnAddressTableBuffer: Array[ByteBuffer] = initColumnAddressTableBuffers()
  private val fistVarLengthColumnOffset = columnBuffers.length - varLengthColumnAddressTableBuffer.length

  private def initColumnBuffers(): Array[ByteBuffer] = {
    this.features.map {
      case CBufferFeature(_, _, _, DataType.Bool, false) => allocateBuffer((options.maxChunkRecordCount + 7) >> 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Byte, false) => allocateBuffer(options.maxChunkRecordCount, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Short, false) => allocateBuffer(options.maxChunkRecordCount << 1, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Int, false) => allocateBuffer(options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Long, false) => allocateBuffer(options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Float, false) => allocateBuffer(options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.Double, false) => allocateBuffer(options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Byte, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Short, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount << 1, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Int, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Long, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Float, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, 0, DataType.Double, true) => allocateBuffer(options.defaultVarColumnScaleRatio * options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Byte, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Short, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 1, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Int, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Long, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Float, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 2, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, arrayLength, DataType.Double, true) => allocateBuffer(arrayLength * options.maxChunkRecordCount << 3, options.useOffHeap, options.bigEndian)
      case CBufferFeature(_, _, _, DataType.String, false) => allocateBuffer(options.maxChunkRecordCount << 4, options.useOffHeap, options.bigEndian)
      case _ => throw new UnsupportedOperationException(s"this should not happen")
    }
  }

  private def initColumnAddressTableBuffers(): Array[ByteBuffer] = {
    // string or var-length array
    this.features
      .filter(e => e.dataType == DataType.String || (e.isArray && e.offset == 0))
      .map(_ => allocateBuffer(options.maxChunkRecordCount * BytesToKeepAddressInChunk, options.useOffHeap, options.bigEndian))
  }

  override def write(value: InternalRow): Unit = {
    ordinals.foreach {
      case (CBufferFeature(_, _, _, DataType.Bool, false), ordinal, featureIndex) =>
        if ((this.size & 7) == 0) {
          this.columnBuffers(featureIndex).put(0.byteValue())
        }
        if (value.getBoolean(ordinal)) {
          this.columnBuffers(featureIndex).put(this.size >> 3, (this.columnBuffers(featureIndex).get(this.size >> 3) | (1 << (this.size & 7))).toByte)
        }
      case (CBufferFeature(_, _, _, DataType.Byte, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).put(value.getByte(ordinal))
      case (CBufferFeature(_, _, _, DataType.Short, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).putShort(value.getShort(ordinal))
      case (CBufferFeature(_, _, _, DataType.Int, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).putInt(value.getInt(ordinal))
      case (CBufferFeature(_, _, _, DataType.Long, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).putLong(value.getLong(ordinal))
      case (CBufferFeature(_, _, _, DataType.Float, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).putFloat(value.getFloat(ordinal))
      case (CBufferFeature(_, _, _, DataType.Double, false), ordinal, featureIndex) =>
        this.columnBuffers(featureIndex).putDouble(value.getDouble(ordinal))
      case (CBufferFeature(_, _, 0, DataType.Byte, true), ordinal, featureIndex) =>
        val bytes = value.getBinary(ordinal)
        checkBufferBound(featureIndex, bytes.length)
        this.columnBuffers(featureIndex).put(bytes)
      case (CBufferFeature(_, _, 0, DataType.Short, true), ordinal, featureIndex) =>
        writeVarLengthArray(value, DataType.Short, ordinal, featureIndex, (arr, i) =>
          this.columnBuffers(featureIndex).putShort(arr.getShort(i)))
      case (CBufferFeature(_, _, 0, DataType.Int, true), ordinal, featureIndex) =>
        writeVarLengthArray(value, DataType.Int, ordinal, featureIndex, (arr, i) =>
          this.columnBuffers(featureIndex).putInt(arr.getInt(i)))
      case (CBufferFeature(_, _, 0, DataType.Long, true), ordinal, featureIndex) =>
        writeVarLengthArray(value, DataType.Long, ordinal, featureIndex, (arr, i) =>
          this.columnBuffers(featureIndex).putLong(arr.getLong(i)))
      case (CBufferFeature(_, _, 0, DataType.Float, true), ordinal, featureIndex) =>
        writeVarLengthArray(value, DataType.Float, ordinal, featureIndex, (arr, i) =>
          this.columnBuffers(featureIndex).putFloat(arr.getFloat(i)))
      case (CBufferFeature(_, _, 0, DataType.Double, true), ordinal, featureIndex) =>
        writeVarLengthArray(value, DataType.Double, ordinal, featureIndex, (arr, i) =>
          this.columnBuffers(featureIndex).putDouble(arr.getDouble(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Byte, true), ordinal, featureIndex) =>
        val bytes = value.getBinary(ordinal)
        assert(arrayLength == bytes.length, "fixed array feature length must be equal as defined")
        this.columnBuffers(featureIndex).put(bytes)
      case (CBufferFeature(_, _, arrayLength, DataType.Short, true), ordinal, featureIndex) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.columnBuffers(featureIndex).putShort(arr.getShort(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Int, true), ordinal, featureIndex) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.columnBuffers(featureIndex).putInt(arr.getInt(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Long, true), ordinal, featureIndex) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.columnBuffers(featureIndex).putLong(arr.getLong(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Float, true), ordinal, featureIndex) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.columnBuffers(featureIndex).putFloat(arr.getFloat(i)))
      case (CBufferFeature(_, _, arrayLength, DataType.Double, true), ordinal, featureIndex) =>
        writeFixedLengthArray(value, arrayLength, ordinal, (arr, i) =>
          this.columnBuffers(featureIndex).putDouble(arr.getDouble(i)))
      case (CBufferFeature(_, _, _, DataType.String, false), ordinal, featureIndex) =>
        val bytes = value.getUTF8String(ordinal).getBytes
        checkBufferBound(featureIndex, bytes.length)
        this.columnBuffers(featureIndex).put(bytes)
      case _ => throw new UnsupportedOperationException(s"this should not happen")
    }

    for (i <- this.varLengthColumnAddressTableBuffer.indices) {
      this.varLengthColumnAddressTableBuffer(i).putInt(this.columnBuffers(i + fistVarLengthColumnOffset).position())
    }
    this.size += 1
  }

  private def checkBufferBound(featureIndex: Int, dataSize: Int): Unit = {
    val offset = this.columnBuffers(featureIndex).position() + dataSize
    if (offset > this.columnBuffers(featureIndex).capacity()) {
      if (options.fixedChunkBuffer) {
        throw new UnsupportedOperationException(s"current chunk size is out of capacity ${this.columnBuffers(featureIndex).capacity()}")
      }
      println(s"chunk buffer extended original size ${this.columnBuffers(featureIndex).capacity()} expectation ${offset}")
      val newChunkBuffer = allocateBuffer(nextAllocateSize(offset), this.options.useOffHeap, this.options.bigEndian)
      val position = this.columnBuffers(featureIndex).position()
      this.columnBuffers(featureIndex).rewind()
      newChunkBuffer.put(this.columnBuffers(featureIndex))
      newChunkBuffer.position(position)
      ByteBufferUtil.clean(this.columnBuffers(featureIndex))
      this.columnBuffers(featureIndex) = newChunkBuffer
    }
  }

  private def writeVarLengthArray(value: InternalRow, dataType: DataType, ordinal: Int, featureIndex: Int, op: (ArrayData, Int) => Unit): Unit = {
    val arr = value.getArray(ordinal)
    val size = if (arr == null) 0 else arr.numElements()
    if (size != 0) {
      checkBufferBound(featureIndex, size * byteWidthOfType(dataType))
      for (i <- 0 until size) {
        op(arr, i)
      }
    }
  }

  override def flush(outputStream: OutputStream): Int = {
    // actual data size + var length column address table size
    val dataChunkSize = this.columnBuffers.map(_.position()).sum + this.size * this.varLengthColumnAddressTableBuffer.length * BytesToKeepAddressInChunk
    // chunk data offset + column address table offset + actual data size + var length column address table size
    val totalChunkSize = ChunkDataOffset + this.features.length * BytesToKeepAddressInChunk + dataChunkSize
    this.sizeBuffer.putInt(DataStart, this.size)
    this.sizeBuffer.putInt(ChunkDataSizeOffset, dataChunkSize)
    var columnAddressOffset = 0
    for (i <- this.features.indices) {
      this.columnOffsetByteBuffer.putInt(columnAddressOffset)
      if (i >= this.fistVarLengthColumnOffset) {
        // var-length data
        columnAddressOffset = columnAddressOffset + this.varLengthColumnAddressTableBuffer(i - this.fistVarLengthColumnOffset).position()
      }
      columnAddressOffset = columnAddressOffset + this.columnBuffers(i).position()
    }

    outputStream.write(this.sizeBuffer.array())
    outputStream.write(this.columnOffsetByteBuffer.array())

    for (i <- this.features.indices) {
      if (i >= this.fistVarLengthColumnOffset) {
        // var-length data
        outputStream.write(this.varLengthColumnAddressTableBuffer(i - this.fistVarLengthColumnOffset).array(), DataStart, this.varLengthColumnAddressTableBuffer(i - this.fistVarLengthColumnOffset).position())
      }
      outputStream.write(this.columnBuffers(i).array(), DataStart, this.columnBuffers(i).position())
    }

    this.reset()
    totalChunkSize
  }

  override def reset(): Unit = {
    this.size = 0
    this.columnBuffers.foreach(_.position(0))
    this.varLengthColumnAddressTableBuffer.foreach(_.position(0))
    this.columnOffsetByteBuffer.position(0)
  }
}
