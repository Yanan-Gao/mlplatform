package com.thetradedesk

import com.thetradedesk.spark.util.TTDConfig.config

import java.time.LocalDate

package object audience {
  var date = config.getDate("date" , LocalDate.now())
  var ttdEnv = config.getString("ttd.env" , "dev")
  val trainSetDownSampleFactor = config.getInt("trainSetDownSampleFactor", default = 2)
  val sampleHit = config.getString("sampleHit", "0")
}
