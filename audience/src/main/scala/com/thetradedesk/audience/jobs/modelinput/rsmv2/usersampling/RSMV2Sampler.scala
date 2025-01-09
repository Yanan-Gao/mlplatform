package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig.{RSMV2UserSampleRatio, RSMV2UserSampleSalt}
import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object RSMV2Sampler extends Sampler {

  override def samplingFunction(symbol: Symbol): Column = {
    shouldTrackTDID(symbol) &&
      substring(symbol, 9, 1) === lit("-") &&
      (abs(xxhash64(concat(symbol, lit(RSMV2UserSampleSalt)))) % lit(10) < lit(RSMV2UserSampleRatio))
  }

}
