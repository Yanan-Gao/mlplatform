package com.thetradedesk.audience.sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object DownSample {
  def hashSampleV2(origin: DataFrame, columnName: String, modulo: Int, seed: String, hits: String, sampleSlotName: String = "sampleSlot"): DataFrame = {
    hashSampleV1(origin, columnName, modulo, seed, Some(hits.split(",").map(_.toInt)), sampleSlotName)
  }

  def hashSampleV1(origin: DataFrame, columnName: String, modulo: Int, seed: String, hits: Option[Array[Int]], sampleSlotName: String = "sampleSlot"): DataFrame = {
    if (modulo <= 1) {
      origin.withColumn(sampleSlotName, lit(0))
    } else {
      origin
        .withColumn(sampleSlotName, abs(hash(concat(col(columnName), lit(seed)))) % modulo)
        .filter(col(sampleSlotName).isin(hits.getOrElse(Array(0)): _*)
        )
    }
  }
}
