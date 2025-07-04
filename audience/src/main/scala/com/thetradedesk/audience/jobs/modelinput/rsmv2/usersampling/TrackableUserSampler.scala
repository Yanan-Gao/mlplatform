package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import com.thetradedesk.audience.shouldTrackTDID
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Column

object TrackableUserSampler extends Sampler {

  override def samplingFunction(symbol: Symbol): Column = shouldTrackTDID(symbol)
}
