package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.features.Features._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

import scala.collection.mutable

object AggregateTransform {

  /**
  * Remember to Add Unit Tests in `AggregateTransformTest.scala` When Adding New Aggregation Functions
  */

  case class TopElementsBuffer(counts: mutable.Map[String, Int])

  class TopElementsAggregator(n: Int) extends Aggregator[String, TopElementsBuffer, Seq[String]] {
    // Initialize the buffer
    override def zero: TopElementsBuffer = TopElementsBuffer(mutable.Map.empty[String, Int])

    // Update the buffer with a new input
    override def reduce(buffer: TopElementsBuffer, element: String): TopElementsBuffer = {
      if (element != null) {
        buffer.counts(element) = buffer.counts.getOrElse(element, 0) + 1
      }
      buffer
    }

    // Merge two buffers
    override def merge(b1: TopElementsBuffer, b2: TopElementsBuffer): TopElementsBuffer = {

      b2.counts.foreach { case (element, count) =>
        if (element != null) {
          b1.counts(element) = b1.counts.getOrElse(element, 0) + count
        }
      }
      b1
    }

    // Transform the buffer to the final output
    override def finish(reduction: TopElementsBuffer): Seq[String] = {
      reduction.counts.toSeq.sortBy(-_._2).take(n).map(_._1)
    }

    // Encoders for the buffer and the output
    override def bufferEncoder: Encoder[TopElementsBuffer] = Encoders.product[TopElementsBuffer]

    override def outputEncoder: Encoder[Seq[String]] = implicitly(ExpressionEncoder[Seq[String]])
  }

  class SeqTopElementsAggregator(n: Int) extends Aggregator[Seq[String], TopElementsBuffer, Seq[String]] {
    // Initialize the buffer
    override def zero: TopElementsBuffer = new TopElementsBuffer(mutable.Map.empty[String, Int])

    // Update the buffer with a new input
    override def reduce(buffer: TopElementsBuffer, elements: Seq[String]): TopElementsBuffer = {
      if (elements != null) {
        elements.foreach { element =>
          if (element != null) {
            buffer.counts(element) = buffer.counts.getOrElse(element, 0) + 1
          }
        }
      }
      buffer
    }

    // Merge two buffers
    override def merge(b1: TopElementsBuffer, b2: TopElementsBuffer): TopElementsBuffer = {
      b2.counts.foreach { case (element, count) =>
        if (element != null) {
          b1.counts(element) = b1.counts.getOrElse(element, 0) + count
        }
      }
      b1
    }

    // Transform the buffer to the final output
    override def finish(reduction: TopElementsBuffer): Seq[String] = {
      reduction.counts.toSeq.sortBy(-_._2).take(n).map(_._1)
    }

    // Encoders for the buffer and the output
    override def bufferEncoder: Encoder[TopElementsBuffer] = Encoders.product[TopElementsBuffer]

    override def outputEncoder: Encoder[Seq[String]] = implicitly(ExpressionEncoder[Seq[String]])

  }


  def genAggCols(window: Integer, aggSpecs: Array[CategoryFeatAggSpecs]): Array[Column] = {
    val aggCols = aggSpecs.filter(_.aggWindowDay == window)

    if (!aggCols.isEmpty) {
      aggCols.map(c => {
        if (c.dataType.startsWith("array")) {
          val seqTopCol = udaf(new SeqTopElementsAggregator(c.topN))
          seqTopCol(col(c.aggField)).as(c.featureName)
        } else {
          val topCol = udaf(new TopElementsAggregator(c.topN))
          topCol(col(c.aggField)).as(c.featureName)
        }
      })
    } else Array.empty[Column]
  }

  def genAggCols(window: Integer, aggSpecs: Array[ContinuousFeatAggSpecs]): Array[Column] = {
    val aggCols = aggSpecs.filter(_.aggWindowDay == window)
    if (!aggCols.isEmpty) {
      aggCols.flatMap(c => {
        c.aggFunc match {
          case AggFunc.Mean => Array((sum(col(c.aggField)) / count(col(c.aggField))).cast(DoubleType).alias(c.featureName))
          case AggFunc.Avg => Array((sum(col(c.aggField)) / sum(when(col(c.aggField) =!= 0, 1).otherwise(0))).cast(DoubleType).alias(c.featureName))
          case AggFunc.Sum => Array(sum(col(c.aggField)).cast(DoubleType).alias(c.featureName))
          case AggFunc.Count => Array(count(col(c.aggField)).cast(LongType).alias(c.featureName))
          case AggFunc.Median => Array(percentile_approx(col(c.aggField), lit(0.5), lit(1000)).cast(DoubleType).alias(c.featureName))
          case AggFunc.P10 => Array(percentile_approx(col(c.aggField), lit(0.1), lit(1000)).cast(DoubleType).alias(c.featureName))
          case AggFunc.P90 => Array(percentile_approx(col(c.aggField), lit(0.9), lit(1000)).cast(DoubleType).alias(c.featureName))
          case AggFunc.Desc => Array(
            count(col(c.aggField)).cast(LongType).alias(s"${c.featureName}_Count"),
            sum(col(c.aggField)).cast(DoubleType).alias(s"${c.featureName}_Sum"),
            max(col(c.aggField)).cast(DoubleType).alias(s"${c.featureName}_Max"),
            min(col(c.aggField)).cast(DoubleType).alias(s"${c.featureName}_Min"),
            (sum(col(c.aggField)) / count(col(c.aggField))).cast(DoubleType).alias(s"${c.featureName}_Mean"),
            (sum(col(c.aggField)) / sum(when(col(c.aggField) =!= 0, 1).otherwise(0))).cast(DoubleType).alias(s"${c.featureName}_NonzeroMean"),
            percentile_approx(col(c.aggField), lit(0.25), lit(1000)).cast(DoubleType).alias(s"${c.featureName}_P25"),
            percentile_approx(col(c.aggField), lit(0.5), lit(1000)).cast(DoubleType).alias(s"${c.featureName}_P50"),
            percentile_approx(col(c.aggField), lit(0.75), lit(1000)).cast(DoubleType).alias(s"${c.featureName}_P75"),
          )
          case _ => throw new UnsupportedOperationException(s"Unsupported aggregation function: ${c.aggFunc} for ${c.featureName}")
        }
      })
    } else Array.empty[Column]
  }

  def genAggCols(window: Integer, aggSpecs: Array[RatioFeatAggSpecs]): Array[Column] = {
    val aggCols = aggSpecs.filter(_.aggWindowDay == window)
    if (!aggCols.isEmpty) {
      aggCols.map(c => {
        // only calculate ratio on the rows where both numerator & denominator are not null
        val colsNotNull = col(c.aggField).isNotNull && col(c.denomField).isNotNull
        (sum(when(colsNotNull, col(c.aggField)).otherwise(0)) /
            sum(when(colsNotNull, col(c.denomField)).otherwise(0))
        ).cast(DoubleType).alias(c.featureName)
      })
    } else Array.empty[Column]
  }
}