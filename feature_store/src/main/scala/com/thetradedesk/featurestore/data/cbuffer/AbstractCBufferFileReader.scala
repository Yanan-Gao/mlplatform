package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.configs.DataType
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.{allocateBuffer, nextAllocateSize}
import com.thetradedesk.featurestore.data.generators.CustomBufferDataGenerator.{byteWidthOfType, lengthOfType}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

import java.nio.{ByteBuffer, ByteOrder}

abstract class AbstractCBufferFileReader(options: CBufferOptions) extends CBufferFileReader {
  protected var chunkBuffer: ByteBuffer = allocateBuffer(options.defaultChunkRecordSize * options.maxChunkRecordCount, options.useOffHeap, options.bigEndian)
  protected var chunkRecordCount: Int = 0
  protected var shortArrayCache: Array[Short] = new Array[Short](options.defaultVarColumnScaleRatio << 2)
  protected var intArrayCache: Array[Int] = new Array[Int](options.defaultVarColumnScaleRatio << 2)
  protected var longArrayCache: Array[Long] = new Array[Long](options.defaultVarColumnScaleRatio << 2)
  protected var floatArrayCache: Array[Float] = new Array[Float](options.defaultVarColumnScaleRatio << 2)
  protected var doubleArrayCache: Array[Double] = new Array[Double](options.defaultVarColumnScaleRatio << 2)

  protected implicit val byteOrder: ByteOrder = if (options.bigEndian) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN

  protected def copyArray(columnVector: WritableColumnVector, byteBuffer: ByteBuffer, dataType: DataType, index: Int, rowId: Int, byteLength: Int, op: (WritableColumnVector, Int) => Int) : Unit = {
    val arrayLength = fillArray(byteBuffer, dataType, index, byteLength)
    val result = op(columnVector.arrayData(), arrayLength)
    columnVector.putArray(rowId, result, arrayLength)
  }

  private def fillArray(byteBuffer: ByteBuffer, dataType: DataType, index: Int, byteLength: Int): Int = {
    val arrayLength = lengthOfType(dataType, byteLength)
    checkArrayCacheBound(dataType, arrayLength)
    val width = byteWidthOfType(dataType)
    var idx = index
    dataType match {
      case DataType.Short =>
        for (i <- 0 until arrayLength) {
          shortArrayCache.update(i, byteBuffer.getShort(idx))
          idx += width
        }
      case DataType.Int =>
        for (i <- 0 until arrayLength) {
          intArrayCache.update(i, byteBuffer.getInt(idx))
          idx += width
        }
      case DataType.Long =>
        for (i <- 0 until arrayLength) {
          longArrayCache.update(i, byteBuffer.getLong(idx))
          idx += width
        }
      case DataType.Float =>
        for (i <- 0 until arrayLength) {
          floatArrayCache.update(i, byteBuffer.getFloat(idx))
          idx += width
        }
      case DataType.Double =>
        for (i <- 0 until arrayLength) {
          doubleArrayCache.update(i, byteBuffer.getDouble(idx))
          idx += width
        }
      case _ =>
        throw new RuntimeException("this should not happen")
    }
    arrayLength
  }

  private def checkArrayCacheBound(dataType: DataType, resultLength: Int): Unit = {
    dataType match {
      case DataType.Short =>
        if (resultLength > shortArrayCache.length) {
          shortArrayCache = new Array[Short](nextAllocateSize(resultLength))
        }
      case DataType.Int =>
        if (resultLength > intArrayCache.length) {
          intArrayCache = new Array[Int](nextAllocateSize(resultLength))
        }
      case DataType.Long =>
        if (resultLength > longArrayCache.length) {
          longArrayCache = new Array[Long](nextAllocateSize(resultLength))
        }
      case DataType.Float =>
        if (resultLength > floatArrayCache.length) {
          floatArrayCache = new Array[Float](nextAllocateSize(resultLength))
        }
      case DataType.Double =>
        if (resultLength > doubleArrayCache.length) {
          doubleArrayCache = new Array[Double](nextAllocateSize(resultLength))
        }
      case _ =>
        throw new RuntimeException("this should not happen")
    }
  }
}
