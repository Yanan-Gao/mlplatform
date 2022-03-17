package com.ttd.features.util

import com.github.nscala_time
import com.ttd.features.datasets.ReadableDataFrame
import com.github.nscala_time.time.Imports.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadableDataFrameUtils {
  implicit class WrappedReadableDataFrame(val df: DataFrame) extends AnyVal {
    def asReadableDataFrame: ReadableDataFrame = new ReadableDataFrame {
      override val basePath: String = "None"
      override def format(path: String, dateTime: nscala_time.time.Imports.DateTime): String = "None"
      override def read(dateRange: Seq[DateTime])(implicit spark: SparkSession): DataFrame =
        df
    }
  }
}