package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import SamplerConfigUtils._

/** Sampler used for general RSMV2 jobs. The configuration can be any object
  * that exposes `sampleSalt` and `sampleRatio` fields (or their historical
  * counterparts `RSMV2UserSampleSalt` and `RSMV2UserSampleRatio`).
  */
case class RSMV2Sampler(conf: Any) extends Sampler {

  private val sampleSalt: String =
    getString(conf, "sampleSalt")
      .orElse(getString(conf, "RSMV2UserSampleSalt"))
      .getOrElse(throw new IllegalArgumentException("sampleSalt is required"))

  private val sampleRatio: Int =
    getInt(conf, "sampleRatio")
      .orElse(getInt(conf, "RSMV2UserSampleRatio"))
      .getOrElse(throw new IllegalArgumentException("sampleRatio is required"))

  override def samplingFunction(symbol: Symbol): Column = {
    shouldTrackTDID(symbol) &&
      substring(symbol, 9, 1) === lit("-") &&
      (abs(xxhash64(concat(symbol, lit(sampleSalt)))) % lit(10) <
        lit(sampleRatio))
  }

}

