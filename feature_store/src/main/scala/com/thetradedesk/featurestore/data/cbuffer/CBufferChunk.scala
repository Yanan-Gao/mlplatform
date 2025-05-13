package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants._
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.allocateBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StructType

import java.io.OutputStream
import java.nio.ByteBuffer

abstract class CBufferChunk(schema: StructType, features: Array[CBufferFeature], options: CBufferOptions) {
  var size: Int = 0
  protected val sizeBuffer: ByteBuffer = allocateBuffer(ChunkDataOffset, options.useOffHeap, options.bigEndian)
  protected val ordinals: Array[(CBufferFeature, Int, Int)] = featureOrdinals()

  def write(value: InternalRow): Unit

  def reset(): Unit

  def flush(outputStream: OutputStream): Int

  private def featureOrdinals(): Array[(CBufferFeature, Int, Int)] = {
    val fieldToOrd = this.schema
      .fields
      .map(_.name)
      .zipWithIndex
      .toMap

    this.features
      .zipWithIndex
      .map(e => (e._1, fieldToOrd(e._1.name), e._2))
  }

  protected def writeFixedLengthArray(value: InternalRow, arrayLength: Int, ordinal: Int, op: (ArrayData, Int) => Unit): Unit = {
    val arr = value.getArray(ordinal)
    assert(arrayLength == arr.numElements(), "fixed array feature length must be equal as defined")
    for (i <- 0 until arrayLength) {
      op(arr, i)
    }
  }
}
