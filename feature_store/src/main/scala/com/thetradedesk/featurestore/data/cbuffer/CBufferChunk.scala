package com.thetradedesk.featurestore.data.cbuffer

import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants._
import com.thetradedesk.featurestore.data.cbuffer.MemoryHelper.allocateBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}

import java.io.OutputStream
import java.nio.ByteBuffer

abstract class CBufferChunk(schema: StructType, features: Array[CBufferFeature], options: CBufferOptions) {
  var size: Int = 0
  protected val sizeBuffer: ByteBuffer = allocateBuffer(ChunkDataOffset, options.useOffHeap, options.bigEndian)
  protected val ordinals: Array[(CBufferFeature, Int, Int)] = featureOrdinals()
  protected val binaryOrdinals: Set[Int] = binaryFeatureOrdinals()

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

  private def binaryFeatureOrdinals(): Set[Int] = {
    this.schema
      .fields
      .zipWithIndex
      .filter(e => e._1.dataType == BinaryType)
      .map(_._2)
      .toSet
  }

  protected def writeFixedLengthArray(name: String, value: InternalRow, arrayLength: Int, ordinal: Int, op: (ArrayData, Int) => Unit): Unit = {
    val arr = value.getArray(ordinal)
    assert(arrayLength == arr.numElements(), s"fixed array feature $name length ${arr.numElements()} must be equal as defined $arrayLength")
    for (i <- 0 until arrayLength) {
      op(arr, i)
    }
  }
}
