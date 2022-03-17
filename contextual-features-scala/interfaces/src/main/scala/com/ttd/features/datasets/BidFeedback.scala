package com.ttd.features.datasets

import com.github.nscala_time.time.Imports.DateTime

case class BidFeedback(version: Int = 5) extends ReadableDataFrame {
  override val basePath: String = s"s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=$version"

  override def format(path: String, dateTime: DateTime): String = {
    s"${path}/date=${dateTime.toString("yyyyMMdd")}"  // dateTime.toString("HH")
  }
}