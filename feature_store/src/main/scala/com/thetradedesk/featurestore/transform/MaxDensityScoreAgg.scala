package com.thetradedesk.featurestore.transform

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

case class SynteticIdDensityScore(syntheticId: Int, densityScore: Float)
case class Buffer(maxScores: mutable.Map[Int, Float])

object MaxDensityScoreAgg extends Aggregator[Seq[SynteticIdDensityScore], Buffer, Seq[SynteticIdDensityScore]] {

  // Initialize the buffer
  override def zero: Buffer = Buffer(mutable.Map.empty)

  // Update buffer with a new row
  override def reduce(buffer: Buffer, syntheticIdDensityScores: Seq[SynteticIdDensityScore]): Buffer = {
    syntheticIdDensityScores.foreach { pair =>
      val opt = buffer.maxScores.get(pair.syntheticId)

      if (opt.isEmpty || opt.get < pair.densityScore) {
        buffer.maxScores.update(pair.syntheticId, pair.densityScore)
      }
    }
    buffer
  }

  // Merge two buffers
  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b2.maxScores.foreach { case (syntheticId, densityScore) =>
      val opt = b1.maxScores.get(syntheticId)

      if (opt.isEmpty || opt.get < densityScore) {
        b1.maxScores.update(syntheticId, densityScore)
      }
    }

    b1
  }

  // Convert the buffer to the output format
  override def finish(reduction: Buffer): Vector[SynteticIdDensityScore] = {
    reduction.maxScores.toSeq.map { case (syntheticId, densityScore) => SynteticIdDensityScore(syntheticId, densityScore) }.toVector
  }

  // Define encoders for intermediate and output data types
  override def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]
  override def outputEncoder: Encoder[Seq[SynteticIdDensityScore]] = ExpressionEncoder[Seq[SynteticIdDensityScore]]
}