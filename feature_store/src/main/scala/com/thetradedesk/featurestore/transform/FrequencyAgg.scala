package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.transform.AggregateTransform.{BaseAggregator, TopElementsBuffer}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable

object FrequencyAgg {
  // Frequency-based aggregators
  trait IFrequencyAggregator[IN] extends BaseAggregator[IN, TopElementsBuffer, Map[String, Int]] {
    protected val n: Int

    override def bufferEncoder: Encoder[TopElementsBuffer] = Encoders.product[TopElementsBuffer]

    override def outputEncoder: Encoder[Map[String, Int]] = implicitly(ExpressionEncoder[Map[String, Int]])

    override def zero: TopElementsBuffer = TopElementsBuffer(mutable.Map.empty[String, Int])

    protected def updateCounts(buffer: TopElementsBuffer, element: String): TopElementsBuffer = {
      if (element != null) {
        buffer.counts(element) = buffer.counts.getOrElse(element, 0) + 1
      }
      buffer
    }

    override def merge(b1: TopElementsBuffer, b2: TopElementsBuffer): TopElementsBuffer = {
      b2.counts.foreach { case (element, count) =>
        if (element != null) {
          b1.counts(element) = b1.counts.getOrElse(element, 0) + count
        }
      }
      b1
    }

    override def finish(reduction: TopElementsBuffer): Map[String, Int] = {
      if (n < 1) reduction.counts.toMap
      else reduction.counts.toSeq.sortBy(-_._2).take(n).toMap
    }
  }

  class FrequencyMergeAggregator(topN: Int) extends IFrequencyAggregator[Map[String, Int]] {
    override protected val n: Int = topN

    override def reduce(b1: TopElementsBuffer, b2: Map[String, Int]): TopElementsBuffer = {
      b2.foreach { case (element, count) =>
        if (element != null) {
          b1.counts(element) = b1.counts.getOrElse(element, 0) + count
        }
      }
      b1
    }
  }

  class FrequencyAggregator(topN: Int) extends IFrequencyAggregator[String] {

    override protected val n: Int = topN

    override def reduce(buffer: TopElementsBuffer, element: String): TopElementsBuffer = {
      updateCounts(buffer, element)
    }
  }

  class ArrayFrequencyAggregator(topN: Int) extends IFrequencyAggregator[Seq[String]] {

    override protected val n: Int = topN

    override def reduce(buffer: TopElementsBuffer, elements: Seq[String]): TopElementsBuffer = {
      if (elements != null) {
        elements.foreach(element => updateCounts(buffer, element))
      }
      buffer
    }
  }

}
