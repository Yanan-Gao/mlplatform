package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.transform.AggregateTransform.BaseAggregator
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object VectorDescriptionAgg {

  val IndexCounts = 0
  val IndexSums = 1
  val IndexMins = 2
  val IndexMaxs = 3


  case class VectorDescBuffer(val size: Int,
                              var counts: Array[Double],
                              var sums: Array[Double],
                              var mins: Array[Double],
                              var maxs: Array[Double],
                              var isEmpty: Boolean = true) {

    def add(element: Array[Double]): Unit = {
      if (element == null || element.isEmpty) {
        return
      }

      if (isEmpty) {
        counts = Array.fill(size)(1)
        sums = element
        mins = element
        maxs = element
        isEmpty = false
      } else {
        counts = counts.zip(element).map { case (c1, c2) => c1 + 1 }
        sums = sums.zip(element).map { case (s1, s2) => s1 + s2 }
        mins = mins.zip(element).map { case (m1, m2) => math.min(m1, m2) }
        maxs = maxs.zip(element).map { case (m1, m2) => math.max(m1, m2) }
      }
    }

    def add(element: Array[Array[Double]]): Unit = {
      if (element == null || element.isEmpty) {
        return
      }

      if (isEmpty) {
        counts = element(IndexCounts)
        sums = element(IndexSums)
        mins = element(IndexMins)
        maxs = element(IndexMaxs)
        isEmpty = false
      } else {
        counts = counts.zip(element(IndexCounts)).map { case (c1, c2) => c1 + c2 }
        sums = sums.zip(element(IndexSums)).map { case (s1, s2) => s1 + s2 }
        mins = mins.zip(element(IndexMins)).map { case (m1, m2) => math.min(m1, m2) }
        maxs = maxs.zip(element(IndexMaxs)).map { case (m1, m2) => math.max(m1, m2) }
      }
    }

    def merge(other: VectorDescBuffer): Unit = {
      if (other.isEmpty) {
        return // do nothing if other is empty
      }

      if (isEmpty) {
        // If this buffer is empty, just copy from other
        counts = other.counts
        sums = other.sums
        mins = other.mins
        maxs = other.maxs
        isEmpty = false
      } else {
        counts = counts.zip(other.counts).map { case (c1, c2) => c1 + c2 }
        sums = sums.zip(other.sums).map { case (s1, s2) => s1 + s2 }
        mins = mins.zip(other.mins).map { case (m1, m2) => math.min(m1, m2) }
        maxs = maxs.zip(other.maxs).map { case (m1, m2) => math.max(m1, m2) }
      }
    }

    def finish: Array[Array[Double]] = {
      if (isEmpty) {
        null
      } else {
        Array(counts, sums, mins, maxs)
      }
    }
  }

  // Description statistics aggregators
  trait VectorDescBaseAggregator[IN] extends BaseAggregator[IN, VectorDescBuffer, Array[Array[Double]]] {
    override def bufferEncoder: Encoder[VectorDescBuffer] = Encoders.product[VectorDescBuffer]

    override def outputEncoder: Encoder[Array[Array[Double]]] = implicitly(ExpressionEncoder[Array[Array[Double]]])

    def bufferSize: Int // Abstract method to be implemented by subclasses

    override def zero: VectorDescBuffer = VectorDescBuffer(bufferSize,
      Array.fill(bufferSize)(0),
      Array.fill(bufferSize)(0),
      Array.fill(bufferSize)(0),
      Array.fill(bufferSize)(0))

    override def merge(b1: VectorDescBuffer, b2: VectorDescBuffer): VectorDescBuffer = {
      b1.merge(b2)
      b1
    }

    override def finish(reduction: VectorDescBuffer): Array[Array[Double]] = {
      reduction.finish
    }
  }


  class VectorDescMergeAggregator(val bufferSize: Int) extends VectorDescBaseAggregator[Array[Array[Double]]] {

    override def reduce(buffer: VectorDescBuffer, element: Array[Array[Double]]): VectorDescBuffer = {
      buffer.add(element)
      buffer
    }
  }

  class VectorDescAggregator(val bufferSize: Int) extends VectorDescBaseAggregator[Array[Double]] {

    override def reduce(buffer: VectorDescBuffer, element: Array[Double]): VectorDescBuffer = {
      buffer.add(element)
      buffer
    }
  }

}
