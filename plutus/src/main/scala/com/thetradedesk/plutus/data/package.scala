package com.thetradedesk.plutus

import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when, xxhash64}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}

import java.time.LocalDate
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

package object data {

  val PLUTUS_DATA_SOURCE = "plutus"
  val DEFAULT_SV_NAME = "google"


  def paddedDatePart(date: LocalDate, separator: Option[String] = Some("")): String = {
    separator match {
      case Some(s) => f"${date.getYear}$s${date.getMonthValue}%02d$s${date.getDayOfMonth}%02d"
    }
  }

  def explicitDatePart(date: LocalDate): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}%02d/day=${date.getDayOfMonth}%02d"
  }

  def parquetDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None): Seq[String] = {
    source match {
      case Some(PLUTUS_DATA_SOURCE) => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDatePart(date)}")
      case _ => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${paddedDatePart(date)}")
    }
  }

  def parquetHourlyDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, hours: Seq[Int]): Seq[String] = {
    var paddedHours: ArrayBuffer[String] = ArrayBuffer.empty[String]
    hours.map{i => if (i < 10) {
      paddedHours += (f"0${i}")
    }
      else {
        paddedHours += i.toString
      }
    }
    paddedHours.map(i => f"$s3path/date=${paddedDatePart(date)}/hour=${i}")
  }

  def cleansedDataPaths(basePath: String, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${basePath}/${explicitDatePart(date.minusDays(i))}/*/*/*.gz")
  }


  def plutusDataPath(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate): String = {
    s"$s3Path/$ttdEnv/$prefix/${svName.getOrElse(DEFAULT_SV_NAME)}/${explicitDatePart(date)}"
  }

  def plutusDataPaths(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${plutusDataPath(s3Path, ttdEnv, prefix, svName, date.minusDays(i))}")
  }

  def loadParquetData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = parquetDataPaths(s3path, date, source, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  def loadParquetDataHourly[T: Encoder](s3path: String, date: LocalDate, hours: Seq[Int], source: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = parquetHourlyDataPaths(s3path, date, source, hours)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  def shiftMod(hashValue: Long, cardinality: Int): Int = {
    val modulo = math.min(cardinality - 1, Int.MaxValue - 1)

    val index = (hashValue % modulo).intValue()
    // zero index is reserved for UNK and we do not want negative values
    val shift = if (index < 0) modulo + 1 else 1
    index + shift
  }

  def shiftModUdf: UserDefinedFunction = udf((hashValue: Long, cardinality: Int) => {
    shiftMod(hashValue, cardinality)
  })

  def cacheToHDFS[T: Encoder](df: Dataset[T], cacheName: String = "unnamed"): Dataset[T] = {
    if (spark.sparkContext.master.contains("local")) {
      //if we are running locally, we are in the middle of a test
      Console.err.println("Skipping cacheToHDFS of " + cacheName)
      df
    } else {
      // TODO: add another unpersist method so we can delete HDFS cached datasets that have this random UUID
      val randomTempPath = "hdfs:///user/hadoop/output-temp-dir" + s"$cacheName-${UUID.randomUUID().toString}"
      df.write.parquet(randomTempPath)
      spark.read.parquet(randomTempPath).as[T]
    }
  }



  val vec_size: UserDefinedFunction = udf((v: Vector) => v.size)
  val vec_indices: UserDefinedFunction = udf((v: SparseVector) => v.indices)
  val vec_values: UserDefinedFunction = udf((v: SparseVector) => v.values)
}
