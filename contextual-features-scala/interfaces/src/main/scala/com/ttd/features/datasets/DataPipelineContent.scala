package com.ttd.features.datasets

import com.github.nscala_time.time.Imports.DateTime

case class DataPipelineContent(version: Int = 1) extends ReadableDataFrame {
  override val basePath: String = s"s3://ttd-datapipe-data/parquet/cxt_content/v=$version"

  override def format(path: String, dateTime: DateTime): String = {
    s"${path}/date=${dateTime.toString("yyyyMMdd")}"  // dateTime.toString("HH")
  }
}
