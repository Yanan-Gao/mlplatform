package com.ttd.benchmarks.util.logging

import com.github.nscala_time.time.Imports
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.FeatureHasher

object S3Paths {
  val AgeAndGenderBenchmarkPath: (String, String) => String = (date, region) =>
    s"s3://ttd-datprd-us-east-1/dev/alex.navarro/application/radar/output/Date=$date/Country=$region/intermediate/userIdAndLabelSave/"

  val Avails7Day = "s3://ttd-identity/datapipeline/prod/avails7day/v=2/"
  val Avails30Day = "s3://ttd-identity/datapipeline/prod/avails30day/v=2/"
  val BidFeedback = "s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=4/"
  val BidRequest = "s3://ttd-datapipe-data/parquet/rtb_bidrequest_cleanfile/v=4/"
  val ClickTracker = "s3://ttd-datapipe-data/parquet/rtb_clicktracker_cleanfile/v=5/"
  val ConversionTracker = "s3://ttd-datapipe-data/parquet/rtb_conversiontracker_cleanfile/v=4/"
  val CrossDeviceGraph = "s3://thetradedesk-useast-data-import/sxd-etl/universal"
  val EventTracker = "s3://ttd-datapipe-data/parquet/rtb_eventtracker_verticaload/v=3/"
  val IdentityAvails = "s3://ttd-identity/datapipeline/sources/avails-idnt/"
  val VideoEvent = "s3://ttd-datapipe-data/parquet/rtb_videoevent_cleanfile/v=4/"
}

trait DatePartitionedDataset {
  val basePath: String

  def dateFormat(date: DateTime): String

  def datePath(date: DateTime): String = {
    basePath + dateFormat(date)
  }

  def readDays(hours: Seq[DateTime])(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(hours map datePath:_*)
  }
}

trait DateHourPartitionedDataset extends DatePartitionedDataset {
  def hourFormat(date: DateTime): String

  def dateHourPath(date: DateTime): String = {
    basePath + dateFormat(date) + hourFormat(date)
  }

  def readHours(hours: Seq[DateTime])(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(hours map dateHourPath:_*)
  }
}

trait RegionDatePartitionedDataset {
  def regionDatePath(region: String, date: DateTime): String

  def readRegionDates(region: String, hours: Seq[DateTime])(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(hours map (h => regionDatePath(region, h)):_*)
  }
}

object AgeAndGenderLabels extends RegionDatePartitionedDataset {
  override def regionDatePath(region: String, date: Imports.DateTime): String = {
    s"s3://ttd-datprd-us-east-1/dev/alex.navarro/application/radar/output" +
      s"/Date=${date.toString("yyyy-MM-dd")}" +
      s"/Country=$region/intermediate/userIdAndLabelSave/"
  }
}

object BidFeedback extends DateHourPartitionedDataset {
  override val basePath = "s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=4/"
  override def dateFormat(date: DateTime): String = "/date=" + date.toString("yyyyMMdd")
  override def hourFormat(date: DateTime): String = "/hour=" + date.toString("HH")
}

object ClickTracker extends DateHourPartitionedDataset {
  override val basePath = "s3://ttd-datapipe-data/parquet/rtb_clicktracker_cleanfile/v=5/"
  override def dateFormat(date: DateTime): String = "/date=" + date.toString("yyyyMMdd")
  override def hourFormat(date: DateTime): String = "/hour=" + date.toString("HH")
}

object IdentityAvails extends DateHourPartitionedDataset {
  override val basePath = "s3://ttd-identity/datapipeline/sources/avails-idnt/"
  override def dateFormat(date: DateTime): String = date.toString("yyyy-MM-dd")
  override def hourFormat(date: DateTime): String = date.toString("HH")
}