package com.thetradedesk.audience.jobs

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class ElementWiseAggregator(arrayLength: Int, aggFun: (Float, Float) => Float, initVal:Float) extends Aggregator[Array[Float], Array[Float], Array[Float]] {

  def zero: Array[Float] = Array.fill(arrayLength)(initVal)

  def reduce(buffer: Array[Float], data: Array[Float]): Array[Float] = {
    for (i <- buffer.indices) {
      buffer(i) = aggFun(buffer(i), data(i))
    }
    buffer
  }

  def merge(b1: Array[Float], b2: Array[Float]): Array[Float] = {
    for (i <- b1.indices) {
      b1(i) = aggFun(b1(i), b2(i))
    }
    b1
  }

  def finish(reduction: Array[Float]): Array[Float] = reduction

  def bufferEncoder: Encoder[Array[Float]] = Encoders.kryo[Array[Float]]
  def outputEncoder: Encoder[Array[Float]] = implicitly(ExpressionEncoder[Array[Float]])
}
