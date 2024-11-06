package com.thetradedesk.audience.utils

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

object BitwiseOrAgg extends Aggregator[Int, Int, Int]{
  override def zero: Int = 0

  override def reduce(b: Int, a: Int): Int = a | b

  override def merge(b1: Int, b2: Int): Int = b1 | b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

object BitwiseAndAgg extends Aggregator[Int, Int, Int]{
  override def zero: Int = 0

  override def reduce(b: Int, a: Int): Int = a & b

  override def merge(b1: Int, b2: Int): Int = b1 & b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

object MapDensity {

  def getTopDensityFeatures(threshold: Double)  = udf { (FeatureCounts: Map[String, Long]) =>
    val densities = if (FeatureCounts == null || FeatureCounts.isEmpty) {
      Map.empty[String, Double]
    } else {
      val totalCount = FeatureCounts.values.sum.toDouble
      if (totalCount == 0) {
        FeatureCounts.map { case (feature, _) => (feature, 0.0) }
      } else {
        FeatureCounts.map { case (feature, count) =>
          val density = count / totalCount
          (feature, density)
        }
      }
    }
  if (densities == null || densities.isEmpty) {
      Seq.empty[String]
    } else {
      val sortedFeatures = densities.toSeq.sortBy(-_._2)  
      var cumulativeSum = 0.0
      val selectedFeatures = scala.collection.mutable.ArrayBuffer[String]()
      var done = false
      for ((feature, density) <- sortedFeatures if !done) {
        selectedFeatures += feature
        cumulativeSum += density
        if (cumulativeSum >= threshold) {
          done = true
        }
      }
      selectedFeatures.toSeq
    }
  }

}