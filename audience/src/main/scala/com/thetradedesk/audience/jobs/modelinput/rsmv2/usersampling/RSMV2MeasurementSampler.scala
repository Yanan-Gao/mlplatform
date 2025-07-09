package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig
import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object RSMV2MeasurementSampler extends Sampler {

  override def samplingFunction(symbol: Symbol, conf: RelevanceModelInputGeneratorJobConfig): Column = {
    val hashedValue = abs(xxhash64(concat(symbol, lit(conf.RSMV2UserSampleSalt)))) % lit(10)

    shouldTrackTDID(symbol) &&
      substring(symbol, 9, 1) === lit("-") &&
      (hashedValue >= lit(conf.RSMV2UserSampleRatio)) &&
      (hashedValue < (lit(conf.RSMV2UserSampleRatio) + lit(1)))
  }
}
