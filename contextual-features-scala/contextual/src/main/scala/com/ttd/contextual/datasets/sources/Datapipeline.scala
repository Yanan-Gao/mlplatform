package com.ttd.contextual.datasets.sources

import com.github.nscala_time
import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports._
import com.ttd.contextual.spark.TTDSparkContext.spark
import org.apache.spark.sql.DataFrame

import scala.collection.immutable

trait ReadableDataFrame {
  val basePath: String

  // format should always format dateTimes into the smallest grain that this dataset can read
  def format(path: String, dateTime: DateTime): String

  def read(dateRange: Seq[DateTime]): DataFrame = {
    spark
      .read
      .option("basePath", basePath)
      .option("mergeSchema", value = false)
      .parquet(dateRange.map(format(basePath, _)):_*)
  }
}

case class DataPipelineContent(version: Int = 1) extends ReadableDataFrame {
  override val basePath: String = s"s3://ttd-datapipe-data/parquet/cxt_content/v=$version"

  override def format(path: String, dateTime: DateTime): String = {
     s"${path}/date=${dateTime.toString("yyyyMMdd")}"  // dateTime.toString("HH")
  }
}

case class DataPipelineTokenizedContent(version: Int = 1) extends ReadableDataFrame {
  override val basePath: String = s"s3://ttd-datapipe-data/parquet/cxt_tokenized_content/v=$version"

  override def format(path: String, dateTime: DateTime): String = {
    s"${path}/date=${dateTime.toString("yyyyMMdd")}"
  }
}

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

  override def read(dateRange: Seq[DateTime]): DataFrame = {
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

case class UrlHistory() extends ReadableDataFrame {
  override val basePath: String = "s3://ttd-identity/datapipeline/sources/avails-idnt"

  override def format(path: String, dateTime: nscala_time.time.Imports.DateTime): String = {
    s"${basePath}/${dateTime.toString("yyyy-MM-dd")}/${dateTime.toString("HH")}"
  }
}

case class YakeKeyphrases() extends ReadableDataFrame {
  override val basePath: String = "s3://ttd-identity/datapipeline/sources/avails-idnt"

  override def format(path: String, dateTime: nscala_time.time.Imports.DateTime): String = {
    s"${basePath}/${dateTime.toString("yyyy-MM-dd")}/${dateTime.toString("HH")}"
  }
}

