package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import SamplerConfigUtils._

/** Sampler used for generating population statistics. Configuration requires
  * a `sampleSalt` and a sequence of `sampleIndexes` indicating which hashed
  * buckets should be kept.
  */
case class RSMV2PopulationSampler(conf: Any) extends Sampler {

  private val sampleSalt: String =
    getString(conf, "sampleSalt")
      .orElse(getString(conf, "RSMV2UserSampleSalt"))
      .getOrElse(throw new IllegalArgumentException("sampleSalt is required"))

  private val sampleIndexes: Seq[Int] =
    getSeqInt(conf, "sampleIndexes")
      .orElse(getSeqInt(conf, "RSMV2PopulationUserSampleIndex"))
      .getOrElse(throw new IllegalArgumentException("sampleIndexes are required"))

  override def samplingFunction(symbol: Symbol): Column = {
    val hashedValue = abs(xxhash64(concat(symbol, lit(sampleSalt)))) % lit(10)

    shouldTrackTDID(symbol) &&
      substring(symbol, 9, 1) === lit("-") &&
      (hashedValue.isin(sampleIndexes: _*))
  }
}

