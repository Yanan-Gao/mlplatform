package com.thetradedesk.geronimo

import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import com.thetradedesk.spark.TTDSparkContext.spark
import java.time.LocalDate
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

package object shared {

    val GERONIMO_DATA_SOURCE = "geronimo"

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
        case Some(GERONIMO_DATA_SOURCE) => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDatePart(date)}")
        case _ => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${paddedDatePart(date)}")
      }
    }

    def parquetHourlyDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, hours: Seq[Int]): Seq[String] = {
      var paddedHours: ArrayBuffer[String] = ArrayBuffer.empty[String]
      hours.map { i =>
        if (i < 10) {
          paddedHours += (f"0${i}")
        }
        else {
          paddedHours += i.toString
        }
      }
      paddedHours.map(i => f"$s3path/date=${paddedDatePart(date)}/hour=${i}")
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

    val STRING_FEATURE_TYPE = "string"
    val INT_FEATURE_TYPE = "int"
    val FLOAT_FEATURE_TYPE = "float"

    def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
      inputColAndDims.map {
        case ModelFeature(name, STRING_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
        case ModelFeature(name, INT_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
        case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _) => col(name).alias(name)
      }.toArray
    }

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

}
