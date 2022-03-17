package com.ttd.features.datasets

import com.github.nscala_time
import com.github.nscala_time.time.Imports.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

case class IdentityAvails(readHours: Boolean = false) extends ReadableDataFrame {
  override val basePath: String = "s3://ttd-identity/datapipeline/sources/avails-idnt"

  override def format(path: String, dateTime: nscala_time.time.Imports.DateTime): String = {
    s"${formatDay(path, dateTime)}/${dateTime.toString("HH")}"
  }

  def formatDay(path: String, dateTime: nscala_time.time.Imports.DateTime): String = {
    s"${basePath}/${dateTime.toString("yyyy-MM-dd")}"
  }

  val dateHours: DateTime => Seq[String] = (date: DateTime) => {
    (0 until 24) map (x => "%02d".format(x)) map (hour => formatDay(basePath, date) + s"/$hour")
  }

  override def read(dateRange: Seq[DateTime])(implicit spark: SparkSession): DataFrame = {
    if (readHours) {
      super.read(dateRange)
    } else {
      // if provided dates are not hours then need to read all hours below each date
      spark
        .read
        .option("basePath", basePath)
        .option("mergeSchema", value = false)
        .parquet(dateRange.flatMap(dateHours(_)):_*)
    }
  }
}
