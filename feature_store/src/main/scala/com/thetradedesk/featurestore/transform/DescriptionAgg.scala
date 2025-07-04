package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.constants.FeatureConstants.ColFeatureKeyCount
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object DescriptionAgg {

  case class VectorDescBuffer(var vector: Array[Double] = Array.empty[Double])

  /**
   * Base trait for vector aggregators with common functionality
   */
  trait VectorDescBaseAggregator extends Aggregator[Array[Double], VectorDescBuffer, Option[Array[Double]]] {
    override def bufferEncoder: Encoder[VectorDescBuffer] = Encoders.product[VectorDescBuffer]
    override def outputEncoder: Encoder[Option[Array[Double]]] = implicitly(ExpressionEncoder[Option[Array[Double]]])
    override def zero: VectorDescBuffer = VectorDescBuffer()
    
    // Abstract method that defines the operation to perform on vector elements
    protected def operation(a: Double, b: Double): Double
    
    override def reduce(buffer: VectorDescBuffer, element: Array[Double]): VectorDescBuffer = {
      if (element == null || element.isEmpty) {
        return buffer
      }
      
      if (buffer.vector.isEmpty) {
        buffer.vector = element
      } else {
        buffer.vector = buffer.vector.zip(element).map { case (a, b) => operation(a, b) }
      }
      buffer
    }
    
    override def merge(b1: VectorDescBuffer, b2: VectorDescBuffer): VectorDescBuffer = {
      if (b2.vector.isEmpty) {
        return b1
      }
      
      if (b1.vector.isEmpty) {
        b1.vector = b2.vector
      } else {
        b1.vector = b1.vector.zip(b2.vector).map { case (a, b) => operation(a, b) }
      }
      b1
    }
    
    override def finish(reduction: VectorDescBuffer): Option[Array[Double]] = {
      if (reduction.vector.isEmpty) None else Some(reduction.vector)
    }
  }

  class VectorSumAggregator extends VectorDescBaseAggregator {
    override protected def operation(a: Double, b: Double): Double = a + b
  }

  class VectorMinAggregator extends VectorDescBaseAggregator {
    override protected def operation(a: Double, b: Double): Double = math.min(a, b)
  }

  class VectorMaxAggregator extends VectorDescBaseAggregator {
    override protected def operation(a: Double, b: Double): Double = math.max(a, b)
  }

  def genKeyCountCol(aLevel: String): Array[Column] = {
    Array(count(col(aLevel)).cast(LongType).alias(ColFeatureKeyCount))
  }
}
