package com.ttd.features.datasets

import com.github.nscala_time.time.Imports.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadableDataFrame {
  val basePath: String

  // format should always format dateTimes into the smallest grain that this dataset can read
  def format(path: String, dateTime: DateTime): String

  def read(dateRange: Seq[DateTime])(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .option("basePath", basePath)
      .option("mergeSchema", value = false)
      .parquet(dateRange.map(format(basePath, _)):_*)
  }
}
