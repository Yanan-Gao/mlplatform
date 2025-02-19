package com.thetradedesk.audience.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class Buffer(sums: Array[Double], var count: Long)

class RelevanceScoresAggregator(arrayLength: Int)
  extends Aggregator[Seq[Float], Buffer, Array[Float]] {

  // Initialize the buffer
  def zero: Buffer = Buffer(Array.fill(arrayLength)(0.0), 0L)

  // Add a new value to the buffer
  def reduce(buffer: Buffer, input: Seq[Float]): Buffer = {
    require(input.length == arrayLength,
      s"Input array length ${input.length} does not match expected length $arrayLength")

    for (i <- 0 until arrayLength) {
      buffer.sums.update(i, buffer.sums(i) + input(i))
    }
    buffer.count = buffer.count + 1
    buffer
  }

  // Merge two buffers
  def merge(b1: Buffer, b2: Buffer): Buffer = {
    for (i <- 0 until arrayLength) {
      b1.sums.update(i, b1.sums(i) + b2.sums(i))
    }

    b1.count = b1.count + b2.count

    b1
  }

  // Calculate final average
  def finish(buffer: Buffer): Array[Float] = {
    if (buffer.count == 0) {
      Array.fill(arrayLength)(0.0f)
    } else {
      buffer.sums.map(sum => (sum / buffer.count).toFloat)
    }
  }

  // Encoders for Spark to serialize the data
  def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]
  def outputEncoder: Encoder[Array[Float]] = ExpressionEncoder[Array[Float]]
}
