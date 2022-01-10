package com.ttd.benchmarks.util

import com.github
import com.github.nscala_time
import com.github.nscala_time.time
import com.github.nscala_time.time.Imports._
import com.ttd.benchmarks.util.logging.{DateHourPartitionedDataset, RegionDatePartitionedDataset}
import org.apache.spark.sql.{DataFrame, SparkSession}

object S3PathsUtil {
  def mockReadDateHour(df: DataFrame): DateHourPartitionedDataset = {
    new DateHourPartitionedDataset {
      override def hourFormat(date: DateTime): String = ???
      override val basePath: String = "???"
      override def dateFormat(date: DateTime): String = ???
      override def readHours(hours: Seq[time.Imports.DateTime])(implicit spark: SparkSession): DataFrame = df
      override def readDays(hours: Seq[nscala_time.time.Imports.DateTime])(implicit spark: SparkSession): DataFrame = df
    }
  }

  def mockReadRegionDate(df: DataFrame): RegionDatePartitionedDataset = {
    new RegionDatePartitionedDataset {
      override def regionDatePath(region: String, date: github.nscala_time.time.Imports.DateTime): String = ""
      override def readRegionDates(region: String, hours: Seq[DateTime])(implicit spark: SparkSession): DataFrame =
        df
    }
  }
}
