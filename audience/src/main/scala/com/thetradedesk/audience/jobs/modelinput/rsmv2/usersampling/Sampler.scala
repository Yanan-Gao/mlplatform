package com.thetradedesk.audience.jobs.modelinput.rsmv2.usersampling

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait Sampler {
  def samplingFunction(symbol: Symbol): Column
}
