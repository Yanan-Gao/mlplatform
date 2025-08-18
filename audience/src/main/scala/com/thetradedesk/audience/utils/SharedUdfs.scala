package com.thetradedesk.audience.utils

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Column}
import java.util.UUID
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

case class AUCHist(
  posCounts: Array[Double],
  negCounts: Array[Double],
  var totalPos: Double,
  var totalNeg: Double
) 

object AUCHist {
  def zero(numBuckets: Int): AUCHist = AUCHist(Array.fill(numBuckets)(0.0), Array.fill(numBuckets)(0.0), 0.0, 0.0)
}

class AUCPerGroupAggregator(numBuckets: Int) extends Aggregator[(Double, Double), AUCHist, Double] {

  def zero: AUCHist = AUCHist.zero(numBuckets)

  def reduce(buffer: AUCHist, input: (Double, Double)): AUCHist = {
    val (pred, label) = input
    val bucket = math.min((pred * numBuckets).toInt, numBuckets - 1)
    if (label == 1.0) {
      buffer.posCounts(bucket) += 1.0
      buffer.totalPos += 1.0
    } else {
      buffer.negCounts(bucket) += 1.0
      buffer.totalNeg += 1.0
    }
    buffer
  }

  def merge(b1: AUCHist, b2: AUCHist): AUCHist = {
    for (i <- 0 until numBuckets) {
      b1.posCounts(i) += b2.posCounts(i)
      b1.negCounts(i) += b2.negCounts(i)
    }
    AUCHist(b1.posCounts, b1.negCounts, b1.totalPos + b2.totalPos, b1.totalNeg + b2.totalNeg)
  }

  def finish(buffer: AUCHist): Double = {
    if (buffer.totalPos == 0 || buffer.totalNeg == 0) {
      0.0 
    } else {
      val cumNeg = new Array[Double](numBuckets)
      cumNeg(0) = buffer.negCounts(0)
      for(i <- 1 until numBuckets) {
        cumNeg(i) = cumNeg(i - 1) + buffer.negCounts(i)
      }
      var U: Double = 0.0
      var ZeroBucketCount: Double = 0.0
      for (i <- 0 until numBuckets) {
        val negativesBelow = if (i == 0) 0L else cumNeg(i - 1)
        U += buffer.posCounts(i) * negativesBelow
        if (buffer.negCounts(i)==0 && buffer.posCounts(i)==0) {
          ZeroBucketCount += 1 
        }
      }
      if (ZeroBucketCount == numBuckets-1) {
        0.5
      } else {
        (U.toDouble / (buffer.totalPos * buffer.totalNeg))
      }
    }
  }

  def bufferEncoder: Encoder[AUCHist] = Encoders.product[AUCHist]
  def outputEncoder: Encoder[Double] = ExpressionEncoder[Double]
}

case class PRCounts(tp: Double, fp: Double, fn: Double)

class precisionRecallAggregator(threshold: Double) extends Aggregator[(Double, Double), PRCounts, (Double, Double)] {
    
    def zero: PRCounts = PRCounts(0.0, 0.0, 0.0)
    
    def reduce(buffer: PRCounts, input: (Double, Double)): PRCounts = {
      val (pred, label) = input
      if (pred >= threshold) {
        if (label == 1.0)
          PRCounts(buffer.tp + 1.0, buffer.fp, buffer.fn)
        else
          PRCounts(buffer.tp, buffer.fp + 1.0, buffer.fn)
      } else {
        if (label == 1.0)
          PRCounts(buffer.tp, buffer.fp, buffer.fn + 1.0)
        else
          buffer
      }
    }
    
    def merge(b1: PRCounts, b2: PRCounts): PRCounts = {
      PRCounts(b1.tp + b2.tp, b1.fp + b2.fp, b1.fn + b2.fn)
    }
    
    def finish(buffer: PRCounts): (Double, Double) = {
      val precision = if (buffer.tp + buffer.fp > 0) buffer.tp.toDouble / (buffer.tp + buffer.fp) else 0.0
      val recall = if (buffer.tp + buffer.fn > 0) buffer.tp.toDouble / (buffer.tp + buffer.fn) else 0.0
      (precision, recall)
    }
    
    def bufferEncoder: Encoder[PRCounts] = Encoders.product[PRCounts]
    def outputEncoder: Encoder[(Double, Double)] = Encoders.tuple(Encoders.scalaDouble, Encoders.scalaDouble)
  }

  object IDTransformUtils {
      ///// UDF sample
    case class Uuid(mostSigBits: Long, leastSigBits: Long)

    def charToLong(char: Char): Long = {
      if (char >= '0' && char <= '9')
        char - '0'
      else if (char >= 'A' && char <= 'F')
        char - ('A' - 0xaL)
      else if (char >= 'a' && char <= 'f')
        char - ('a' - 0xaL)
      else
        0L
    }

    def guidToLongs(tdid: String): Uuid = {
      if (tdid == null) {
        Uuid(0,0)
      } else {
        val hexString = tdid.replace("-", "")

        if (!hexString.matches("[0-9A-Fa-f]{32}")) {
          Uuid(0, 0)
        } else {
          val uid = UUID.fromString(tdid)
          val highBits = uid.getMostSignificantBits
          val lowBits = uid.getLeastSignificantBits

          Uuid(highBits, lowBits)
        }
      }
    }

    def udfGuidToLongs: UserDefinedFunction = udf((id: String) => guidToLongs(id))

    def longsToGuid(tdid1: Long, tdid2: Long): String = {
      new UUID(tdid1, tdid2).toString
    }

    def udfLongsToGuid: UserDefinedFunction = udf((mostSigBits: Long, leastSigBits: Long)  => longsToGuid(mostSigBits,leastSigBits))

    def addGuidBits(colName: String)(df: DataFrame): DataFrame = {
      val tmp = s"${colName}Bits"
      df.withColumn(tmp, udfGuidToLongs(col(colName)))
        .withColumn(s"${colName}mostSigBits",  col(s"$tmp.mostSigBits"))
        .withColumn(s"${colName}leastSigBits", col(s"$tmp.leastSigBits"))
        .drop(tmp)
    }


  }

  