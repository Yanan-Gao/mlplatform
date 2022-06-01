package com.thetradedesk

import com.thetradedesk.spark.util.TTDConfig.config

import java.time.LocalDate

package object audience {
  var date = config.getDate("date" , LocalDate.now())
  var ttdEnv = config.getString("ttd.env" , "dev")
}
