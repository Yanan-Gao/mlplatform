package com.ttd.mycellium.util

import com.ttd.mycellium.spark.config.TTDConfig.config

trait S3AccessConfig {
  val accessKey: String = config.getString("accessKey", "")
  val secretKey: String = config.getString("secretKey", "")
}
