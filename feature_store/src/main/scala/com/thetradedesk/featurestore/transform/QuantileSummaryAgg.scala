package com.thetradedesk.featurestore.transform

import com.tdunning.math.stats.MergingDigest
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import java.nio.ByteBuffer

abstract class BaseQuantileSummaryAgg[IN] extends Aggregator[IN, MergingDigest, Array[Byte]] {

  protected val compression: Double = 100

  override def zero: MergingDigest = new MergingDigest(compression)

  protected def serializeDigest(digest: MergingDigest): Array[Byte] = {
    digest.compress()
    val buffer = ByteBuffer.allocate(digest.smallByteSize())
    digest.asSmallBytes(buffer)
    buffer.array()
  }

  override def finish(reduction: MergingDigest): Array[Byte] = serializeDigest(reduction)

  override def bufferEncoder: Encoder[MergingDigest] = Encoders.kryo[MergingDigest]
  override def outputEncoder: Encoder[Array[Byte]] = ExpressionEncoder[Array[Byte]]
}

class QuantileSummaryAgg extends BaseQuantileSummaryAgg[Double] {

  override def reduce(digest: MergingDigest, value: Double): MergingDigest = {
    digest.add(value)
    digest
  }

  override def merge(d1: MergingDigest, d2: MergingDigest): MergingDigest = {
    d1.add(d2)
    d1
  }
}

class QuantileSummaryMergeAgg extends BaseQuantileSummaryAgg[Array[Byte]] {

  override def reduce(digest: MergingDigest, bytes: Array[Byte]): MergingDigest = {
    digest.add(MergingDigest.fromBytes(ByteBuffer.wrap(bytes)))
    digest
  }

  override def merge(d1: MergingDigest, d2: MergingDigest): MergingDigest = {
    d1.add(d2)
    d1
  }
}
