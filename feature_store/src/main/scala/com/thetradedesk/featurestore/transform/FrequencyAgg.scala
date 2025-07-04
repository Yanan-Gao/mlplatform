package com.thetradedesk.featurestore.transform

import com.thetradedesk.featurestore.transform.AggregateTransform.{BaseAggregator, TopElementsBuffer}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable

object FrequencyAgg {

  type FrequencyCounts = Map[String, Int]

  /**
   * Base trait for frequency-based aggregators
   * Provides common functionality for counting and merging frequency data
   */
  trait IFrequencyAggregator[IN] extends BaseAggregator[IN, TopElementsBuffer, FrequencyCounts] {
    
    /**
     * Maximum number of top elements to return
     */
    protected val topN: Int

    override def bufferEncoder: Encoder[TopElementsBuffer] = Encoders.product[TopElementsBuffer]
    override def outputEncoder: Encoder[FrequencyCounts] = implicitly(ExpressionEncoder[FrequencyCounts])
    override def zero: TopElementsBuffer = TopElementsBuffer(mutable.Map.empty[String, Int])

    /**
     * Updates the count for a single element
     */
    protected def updateCount(buffer: TopElementsBuffer, element: String): TopElementsBuffer = {
      if (element != null) {
        buffer.counts(element) = buffer.counts.getOrElse(element, 0) + 1
      }
      buffer
    }

    /**
     * Updates counts for multiple elements
     */
    protected def updateCounts(buffer: TopElementsBuffer, elements: Iterable[String]): TopElementsBuffer = {
      if (elements != null && !elements.isEmpty) {
        elements.foreach(element => updateCount(buffer, element))
      }
      buffer
    }

    /**
     * Merges two frequency buffers
     */
    override def merge(b1: TopElementsBuffer, b2: TopElementsBuffer): TopElementsBuffer = {
      if (b2.counts.isEmpty) return b1
      
      b2.counts.collect { 
        case (element, count) if element != null => 
          b1.counts(element) = b1.counts.getOrElse(element, 0) + count
      }
      b1
    }

    /**
     * Finalizes the aggregation by returning top N elements
     */
    override def finish(reduction: TopElementsBuffer): FrequencyCounts = {
      reduction.counts match {
        case counts if counts.isEmpty => null
        case counts if topN < 1 => counts.toMap // return all 
        case counts => counts.toSeq
          .sortBy(-_._2) // Sort by count in descending order
          .take(topN)
          .toMap
      }
    }
  }

  /**
   * Aggregator for merging frequency maps (used in rollup operations)
   */
  class FrequencyMergeAggregator(override protected val topN: Int) extends IFrequencyAggregator[FrequencyCounts] {

    override def reduce(buffer: TopElementsBuffer, frequencyMap: FrequencyCounts): TopElementsBuffer = {
      if (frequencyMap == null || frequencyMap.isEmpty) {
        return buffer
      }
      frequencyMap.foreach { case (element, count) =>
        if (element != null) {
          buffer.counts(element) = buffer.counts.getOrElse(element, 0) + count
        }
      }
      buffer
    }
  }

  /**
   * Aggregator for single string values
   */
  class FrequencyAggregator(override protected val topN: Int) extends IFrequencyAggregator[String] {

    override def reduce(buffer: TopElementsBuffer, element: String): TopElementsBuffer = {
      updateCount(buffer, element)
    }
  }

  /**
   * Aggregator for sequences of strings (arrays)
   */
  class ArrayFrequencyAggregator(override protected val topN: Int) extends IFrequencyAggregator[Seq[String]] {

    override def reduce(buffer: TopElementsBuffer, elements: Seq[String]): TopElementsBuffer = {
      updateCounts(buffer, elements)
    }
  }
}
