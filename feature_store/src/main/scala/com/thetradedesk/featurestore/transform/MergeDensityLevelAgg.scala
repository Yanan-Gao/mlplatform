package com.thetradedesk.featurestore.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

case class DensityLevelBuffer(densityLevels: mutable.Map[Int, Float])

case class MergeDensityLevelAgg(densityFeatureLevel2Threshold: Float) extends Aggregator[(Seq[Int], Seq[Int], Int, Int), DensityLevelBuffer, (Seq[Int], Seq[Int])] {

  override def zero: DensityLevelBuffer = {
    val mapBuilder = mutable.HashMap.newBuilder[Int, Float]
    mapBuilder.sizeHint(1024)
    DensityLevelBuffer(mapBuilder.result())
  }

  override def reduce(buffer: DensityLevelBuffer, densityLevels: (Seq[Int], Seq[Int], Int, Int)): DensityLevelBuffer = {
    if (densityLevels != null) {
      val frequency = (densityLevels._3 * 1.0 / densityLevels._4).floatValue()
      densityLevels._2.foreach(e => buffer.densityLevels.update(e, buffer.densityLevels.getOrElse(e, 0f) + frequency))
      densityLevels._1.foreach(buffer.densityLevels.getOrElseUpdate(_, 0f))
    }
    buffer
  }

  override def merge(b1: DensityLevelBuffer, b2: DensityLevelBuffer): DensityLevelBuffer = {
    b1.densityLevels.foreach(
      e =>
        b2.densityLevels.update(e._1, b2.densityLevels.getOrElse(e._1, 0f) + e._2)
    )

    b2
  }

  override def finish(reduction: DensityLevelBuffer): (Seq[Int], Seq[Int]) = {
    (reduction.densityLevels.iterator.filter(e => e._2 < densityFeatureLevel2Threshold).map(_._1).toSeq, reduction.densityLevels.iterator.filter(e => e._2 >= densityFeatureLevel2Threshold).map(_._1).toSeq)
  }

  override def bufferEncoder: Encoder[DensityLevelBuffer] = Encoders.product[DensityLevelBuffer]

  override def outputEncoder: Encoder[(Seq[Int], Seq[Int])] = ExpressionEncoder[(Seq[Int], Seq[Int])]
}