package com.ttd.contextual.util

import com.github
import com.github.nscala_time
import com.ttd.contextual.datasets.sources.ReadableDataFrame
import org.apache.spark.sql.DataFrame

object ReadableDataFrameUtils {
  implicit class WrappedReadableDataFrame(val df: DataFrame) extends AnyVal {
    def asReadableDataFrame: ReadableDataFrame = new ReadableDataFrame {
      override val basePath: String = "None"
      override def format(path: String, dateTime: nscala_time.time.Imports.DateTime): String = "None"
      override def read(dateRange: Seq[github.nscala_time.time.Imports.DateTime]): DataFrame =
        df
    }
  }
}
