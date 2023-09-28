package com.thetradedesk.audience.utils

import com.thetradedesk.spark.util.TTDConfig.config

object Logger {
  private val debug = config.getBoolean("debug", default = false)

  def Log(line: Any): Unit = {
    if (debug) {
      println(line)
    }
  }

  def Log(action: () => Any): Unit = {
    if (debug) {
      println(action())
    }
  }
}
