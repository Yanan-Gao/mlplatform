package com.thetradedesk.featurestore.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

case class SeedCountBuffer(seedIdCounts: mutable.Map[String, Int])
object BatchSeedCountAgg extends Aggregator[Seq[String], SeedCountBuffer, Seq[(String, Int)]] {
  override def zero: SeedCountBuffer = {
    val mapBuilder = mutable.HashMap.newBuilder[String, Int]
    mapBuilder.sizeHint(1024)
    SeedCountBuffer(mapBuilder.result())
  }

  override def reduce(buffer: SeedCountBuffer, keys: Seq[String]): SeedCountBuffer = {
    if (keys != null) {
      keys.foreach(
        e => buffer.seedIdCounts.update(e, buffer.seedIdCounts.getOrElse(e, 0) + 1)
      )
    }
    buffer
  }

  override def merge(b1: SeedCountBuffer, b2: SeedCountBuffer): SeedCountBuffer = {
    b2.seedIdCounts.foreach(
      e => b1.seedIdCounts.update(e._1, b1.seedIdCounts.getOrElse(e._1, 0) + e._2)
    )
    b1
  }

  override def finish(reduction: SeedCountBuffer): Seq[(String, Int)] = {
    reduction.seedIdCounts.iterator.map(e => (e._1, e._2)).toSeq
  }

  override def bufferEncoder: Encoder[SeedCountBuffer] = Encoders.product[SeedCountBuffer]

  override def outputEncoder: Encoder[Seq[(String, Int)]] = ExpressionEncoder[Seq[(String, Int)]]
}