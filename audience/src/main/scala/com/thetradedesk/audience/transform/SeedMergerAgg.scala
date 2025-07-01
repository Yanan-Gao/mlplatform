package com.thetradedesk.audience.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.roaringbitmap.RoaringBitmap

case class SeedMergerAgg(seedIds: Seq[String]) extends Aggregator[Seq[String], RoaringBitmap, Seq[String]] {
  override def zero: RoaringBitmap = new RoaringBitmap()

  private lazy val seedIdWithIdx = seedIds.zipWithIndex
  private lazy val seedIdToIdx = seedIdWithIdx.toMap
  private lazy val idxToSeedId = seedIdWithIdx.map(e => (e._2, e._1)).toMap

  override def reduce(buffer: RoaringBitmap, keys: Seq[String]): RoaringBitmap = {
    if (keys != null) {
      keys.foreach(
        e => {
          val op = seedIdToIdx.get(e)
          if (op.nonEmpty) buffer.add(op.get)
        }
      )
    }
    buffer
  }

  override def merge(b1: RoaringBitmap, b2: RoaringBitmap): RoaringBitmap = {
    b1.or(b2)
    b1
  }

  override def finish(reduction: RoaringBitmap): Seq[String] = {
    reduction.toArray.map(e => idxToSeedId(e))
  }

  override def bufferEncoder: Encoder[RoaringBitmap] = Encoders.kryo[RoaringBitmap]

  override def outputEncoder: Encoder[Seq[String]] = ExpressionEncoder.apply[Seq[String]]
}
