package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import SamplerConfigUtils._

/** Sampler used for measurement jobs. It selects one specific 1/10th bucket
  * determined by a hash of the TDID.
  */
case class RSMV2MeasurementSampler(conf: Any) extends Sampler {

  private val sampleSalt: String =
    getString(conf, "sampleSalt")
      .orElse(getString(conf, "RSMV2UserSampleSalt"))
      .getOrElse(throw new IllegalArgumentException("sampleSalt is required"))

  private val sampleRatio: Int =
    getInt(conf, "sampleRatio")
      .orElse(getInt(conf, "RSMV2UserSampleRatio"))
      .getOrElse(throw new IllegalArgumentException("sampleRatio is required"))

  override def samplingFunction(symbol: Symbol): Column = {
    val hashedValue = abs(xxhash64(concat(symbol, lit(sampleSalt)))) % lit(10)

    shouldTrackTDID(symbol) &&
      substring(symbol, 9, 1) === lit("-") &&
      (hashedValue >= lit(sampleRatio)) &&
      (hashedValue < (lit(sampleRatio) + lit(1)))
  }
}

