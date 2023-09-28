package com.thetradedesk.audience.utils

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

object BitwiseOrAgg extends Aggregator[Int, Int, Int]{
  override def zero: Int = 0

  override def reduce(b: Int, a: Int): Int = a | b

  override def merge(b1: Int, b2: Int): Int = b1 | b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

object BitwiseAndAgg extends Aggregator[Int, Int, Int]{
  override def zero: Int = 0

  override def reduce(b: Int, a: Int): Int = a & b

  override def merge(b1: Int, b2: Int): Int = b1 & b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}
