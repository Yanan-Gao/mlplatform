package com.thetradedesk

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions

package object data {

  def datePaddedPart(date: LocalDate): String = {
    f"${date.getYear}/${date.getMonthValue}%02d/${date.getDayOfMonth}%02d"
  }

  def getParquetData[T: Encoder](date: LocalDate, lookBack: Int, s3path: String)(implicit spark: SparkSession): Dataset[T] = {
    val paths = (1 to lookBack).map{ i =>
      s3path + f"date=${date.minusDays(i).getYear}${date.minusDays(i).getMonthValue}%02d${date.minusDays(i).getDayOfMonth}%02d/"
    }
    val df = spark.read.parquet(paths: _*)
    df.selectAs[T]
  }
}
