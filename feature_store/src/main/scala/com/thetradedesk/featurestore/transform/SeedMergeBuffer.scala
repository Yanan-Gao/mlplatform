package com.thetradedesk.featurestore.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

case class SeedMergeBuffer(seedIds: mutable.Map[String, Boolean])

object SeedMergerAgg extends Aggregator[Seq[String], SeedMergeBuffer, Seq[String]] {
  override def zero: SeedMergeBuffer = {
    val mapBuilder = mutable.HashMap.newBuilder[String, Boolean]
    mapBuilder.sizeHint(1024)
    SeedMergeBuffer(mapBuilder.result())
  }

  override def reduce(buffer: SeedMergeBuffer, keys: Seq[String]): SeedMergeBuffer = {
    if (keys != null) {
      keys.foreach(
        e => buffer.seedIds.update(e, true)
      )
    }
    buffer
  }

  override def merge(b1: SeedMergeBuffer, b2: SeedMergeBuffer): SeedMergeBuffer = {
    b2.seedIds.foreach(
      e => b1.seedIds.update(e._1, true)
    )
    b1
  }

  override def finish(reduction: SeedMergeBuffer): Seq[String] = {
    reduction.seedIds.keys.toSeq
  }

  override def bufferEncoder: Encoder[SeedMergeBuffer] = Encoders.product[SeedMergeBuffer]

  override def outputEncoder: Encoder[Seq[String]] = ExpressionEncoder.apply[Seq[String]]
}