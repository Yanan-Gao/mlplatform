package com.thetradedesk.audience.transform

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

case class DensityLevelBuffer(densityLevels: mutable.Map[Int, Boolean])

object MergeDensityLevelAgg extends Aggregator[(Seq[Int], Seq[Int]), DensityLevelBuffer, (Seq[Int], Seq[Int])] {

  override def zero: DensityLevelBuffer = {
    val mapBuilder = mutable.HashMap.newBuilder[Int, Boolean]
    mapBuilder.sizeHint(1024)
    DensityLevelBuffer(mapBuilder.result())
  }

  override def reduce(buffer: DensityLevelBuffer, densityLevels: (Seq[Int], Seq[Int])): DensityLevelBuffer = {
    if (densityLevels != null) {
      densityLevels._2.foreach(buffer.densityLevels.update(_, true))
      densityLevels._1.foreach(buffer.densityLevels.getOrElseUpdate(_, false))
    }
    buffer
  }

  override def merge(b1: DensityLevelBuffer, b2: DensityLevelBuffer): DensityLevelBuffer = {
    b1.densityLevels.foreach(
      e =>
        if (e._2) b2.densityLevels.update(e._1, true)
        else b2.densityLevels.getOrElseUpdate(e._1, false)
    )

    b2
  }

  override def finish(reduction: DensityLevelBuffer): (Seq[Int], Seq[Int]) = {
    (reduction.densityLevels.iterator.filter(!_._2).map(_._1).toSeq, reduction.densityLevels.iterator.filter(_._2).map(_._1).toSeq)
  }

  override def bufferEncoder: Encoder[DensityLevelBuffer] = Encoders.product[DensityLevelBuffer]

  override def outputEncoder: Encoder[(Seq[Int], Seq[Int])] = ExpressionEncoder[(Seq[Int], Seq[Int])]
}