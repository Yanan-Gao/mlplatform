package com.thetradedesk.audience.sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

object DownSample {
  def hashSample(origin: Dataset[_], col: Column, modulo: Int, hit: Int, seed: String): Dataset[_] = {
    if (modulo <= 1) {
      origin
    } else if (hit < 0 || hit >= modulo) {
      origin.limit(0)
    } else {
      origin.filter(
        abs(hash(concat(col, lit(seed)))) % modulo === hit
      )
    }
  }
}
