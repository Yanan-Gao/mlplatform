package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.transform.AggregateTransform.BaseAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object DescriptionAgg {

  // Description statistics buffer
  case class DescBuffer(
                         var count: Double = 0,
                         var nonZeroCount: Double = 0,
                         var sum: Double = 0,
                         var min: Double = 0,
                         var max: Double = 0,
                         var allNull: Double = 1
                       ) {
    private def updateMinMax(value: Double): Unit = {
      min = if (allNull == 1) value else math.min(min, value)
      max = if (allNull == 1) value else math.max(max, value)
    }

    def add(value: Option[Double]): Unit = {
      value.foreach { v =>
        count += 1
        sum += v
        updateMinMax(v)
        if (v != 0) nonZeroCount += 1
        allNull = 0
      }
    }

    def addSeq(seq: Option[Seq[Double]]): Unit = {
      seq.foreach { s =>
        count += s.size
        sum += s.sum
        updateMinMax(s.min)
        max = math.max(max, s.max)
        nonZeroCount += s.count(_ != 0)
        allNull = 0
      }
    }

    def merge(other: DescBuffer): Unit = {
      if (other.allNull == 0) {
        count += other.count
        nonZeroCount += other.nonZeroCount
        sum += other.sum
        min = if (allNull == 1) other.min else math.min(min, other.min)
        max = if (allNull == 1) other.max else math.max(max, other.max)
        allNull = 0
      }
    }

    def merge(other: Map[String, Double]): Unit = {
      if (other("allNull") == 0) {
        count += other("count")
        nonZeroCount += other("nonZeroCount")
        sum += other("sum")
        min = if (allNull == 1) other("min") else math.min(min, other("min"))
        max = if (allNull == 1) other("max") else math.max(max, other("max"))
        allNull = 0
      }
    }

    def finish: Map[String, Double] = Map(
      "count" -> count,
      "nonZeroCount" -> nonZeroCount,
      "sum" -> sum,
      "min" -> min,
      "max" -> max,
      "allNull" -> allNull
    )
  }

  // Description statistics aggregators
  trait DescBaseAggregator[IN] extends BaseAggregator[IN, DescBuffer, Map[String, Double]] {
    override def bufferEncoder: Encoder[DescBuffer] = Encoders.product[DescBuffer]

    override def outputEncoder: Encoder[Map[String, Double]] = implicitly(ExpressionEncoder[Map[String, Double]])

    override def zero: DescBuffer = DescBuffer()

    override def merge(b1: DescBuffer, b2: DescBuffer): DescBuffer = {
      b1.merge(b2)
      b1
    }

    override def finish(reduction: DescBuffer): Map[String, Double] = {
      reduction.finish
    }
  }


  class DescMergeAggregator extends DescBaseAggregator[Map[String, Double]] {

    override def reduce(buffer: DescBuffer, element: Map[String, Double]): DescBuffer = {
      buffer.merge(element)
      buffer
    }
  }

  class DescAggregator extends DescBaseAggregator[Option[Double]] {

    override def reduce(buffer: DescBuffer, element: Option[Double]): DescBuffer = {
      buffer.add(element)
      buffer
    }
  }

  class ArrayDescAggregator extends DescBaseAggregator[Option[Seq[Double]]] {

    override def reduce(buffer: DescBuffer, element: Option[Seq[Double]]): DescBuffer = {
      buffer.addSeq(element)
      buffer
    }
  }

}
