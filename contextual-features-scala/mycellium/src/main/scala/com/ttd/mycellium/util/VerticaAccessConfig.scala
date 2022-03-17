package com.ttd.mycellium.util

import com.ttd.mycellium.spark.config.TTDConfig.config

trait VerticaAccessConfig {
  val user: String = config.getString("user", "None")
  val password: String = config.getString("password", "None")
  val verticaDateFormat: String = "yyyy-MM-dd"
}
