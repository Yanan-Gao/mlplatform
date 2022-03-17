package com.ttd.features.datasets

import com.github.nscala_time.time.Imports.DateTime

case class ConversionTracker(version: Int = 5) extends ReadableDataFrame {
  override val basePath: String = s"s3://ttd-datapipe-data/parquet/rtb_conversiontracker_cleanfile/v=$version"

  override def format(path: String, dateTime: DateTime): String = {
    s"${path}/date=${dateTime.toString("yyyyMMdd")}"  // dateTime.toString("HH")
  }
}