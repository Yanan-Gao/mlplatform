package com.thetradedesk.plutus

import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, shiftMod, shiftModUdf}
import com.thetradedesk.plutus.data.schema.ModelTarget
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import job.RawInputDataProcessor.date
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when, xxhash64}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}

import java.time.{LocalDate, ZonedDateTime}
import java.time.temporal.TemporalAmount
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

package object data {

  val DATA_VERSION = 2

  val PLUTUS_DATA_SOURCE = "plutus"
  val IMPLICIT_DATA_SOURCE = "bidsimpressions"

  val DEFAULT_SV_NAME = "google"
  val IMPLICIT_SV_NAME = "implicit"
  val DEFAULT_IMPLICIT_PREFIX = "bidsimpressions"

  val DEFAULT_NUM_CSV_PARTITIONS = 20
  val DEFAULT_NUM_PARQUET_PARTITIONS = 500

  val LOSS_CODE_WIN = -1
  val LOSS_CODE_LOST_TO_HIGHER_BIDDER = 102

  val DEFAULT_MODEL_FEATURES: Array[ModelFeature] = Array(
    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("DealId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(15002), 0),
    ModelFeature("SupplyVendorSiteId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(350002), 0),
    ModelFeature("AdFormat", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("Metro", STRING_FEATURE_TYPE, Some(302), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(75002), 0),
    ModelFeature("Zip", STRING_FEATURE_TYPE, Some(90002), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(1002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(10002), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(502), 0),

    // these are already integers
    ModelFeature("RenderingContext", INT_FEATURE_TYPE, Some(6), 0),
    ModelFeature("UserHourOfWeek", INT_FEATURE_TYPE, Some(24 * 7 + 2), 0),
    ModelFeature("AdsTxtSellerType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("PublisherType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("DeviceType", INT_FEATURE_TYPE, Some(9), 0),
    ModelFeature("OperatingSystemFamily", INT_FEATURE_TYPE, Some(10), 0),
    ModelFeature("Browser", INT_FEATURE_TYPE, Some(20), 0),

    ModelFeature("sin_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("latitude", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("longitude", FLOAT_FEATURE_TYPE, None, 0)
  )

  val EXPLICIT_MODEL_TARGETS: Array[ModelTarget] = Array(
    ModelTarget("IsImp", "float", nullable = false),
    ModelTarget("AuctionBidPrice", "float", nullable = false),
    ModelTarget("RealMediaCost", "float", nullable = true),
    ModelTarget("mbtw", "float", nullable = false),
    ModelTarget("FloorPriceInUSD", "float", nullable = true),
  )

  val IMPLICIT_MODEL_TARGETS: Array[ModelTarget] = Array(
    ModelTarget("IsImp", "float", nullable = false),
  )

  val MISSING_DATA_VALUE: Int = -1

  def plutusFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _) => when(col(name).isNotNullOrEmpty, shiftModMaxValueUDF(xxhash64(col(name)))).otherwise(MISSING_DATA_VALUE).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, _, _) => when(col(name).isNotNull, shiftModMaxValueUDF(col(name))).otherwise(MISSING_DATA_VALUE).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _) => col(name).alias(name)
    }.toArray
  }

  def plutusTargetCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }

  def shiftModMaxValueUDF: UserDefinedFunction = udf((hashValue: Long) => {
    shiftMod(hashValue, Int.MaxValue)
  })

  def nonNegativeModulo(hashValue: Long, maybeCardinality: Option[Int] = None): Int = {
    /**
     * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala#L1855
     */
    val m = maybeCardinality.getOrElse(Int.MaxValue)
    ((hashValue % m).intValue() + m) % m
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def modMaxValueUDF: UserDefinedFunction = udf((hashValue: Long) => {
    shiftMod(hashValue, Int.MaxValue)
  })

  def paddedDatePart(date: LocalDate, separator: Option[String] = None): String = {
    separator match {
      case Some(s) => f"${date.getYear}$s${date.getMonthValue}%02d$s${date.getDayOfMonth}%02d"
      case _ => f"${date.getYear}${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
    }
  }

  def explicitDateTimePart(date: ZonedDateTime): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}%02d/day=${date.getDayOfMonth}%02d/hourPart=${date.getHour}%02d"
  }

  def explicitDatePart(date: LocalDate): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}%02d/day=${date.getDayOfMonth}%02d"
  }

  def parquetDataPaths(s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None, sep: Option[String] = None): Seq[String] = {
    source match {
      case Some(PLUTUS_DATA_SOURCE | IMPLICIT_DATA_SOURCE) => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDatePart(date.minusDays(i))}")
      case _ => (0 to lookBack.getOrElse(0)).map(i => f"$s3path/date=${paddedDatePart(date.minusDays(i), separator = sep)}")
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

  def cleansedDataPaths(basePath: String, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${basePath}/${paddedDatePart(date.minusDays(i), separator = Some("/"))}/*/*/*.gz")
  }

  def implicitDataPath(s3Path: String, ttdEnv: String, prefix: Option[String] = None): String = {
    s"$s3Path/$ttdEnv/${prefix.getOrElse(DEFAULT_IMPLICIT_PREFIX)}"
  }

  def plutusDataPath(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate): String = {
    s"$s3Path/$ttdEnv/$prefix/${svName.getOrElse(DEFAULT_SV_NAME)}/${explicitDatePart(date)}"
  }

  def plutusDataPaths(s3Path: String, ttdEnv: String, prefix: String, svName: Option[String] = None, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${plutusDataPath(s3Path, ttdEnv, prefix, svName, date.minusDays(i))}")
  }

  def loadParquetData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None): Dataset[T] = {
    val paths = parquetDataPaths(s3path, date, source, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  def dateRange(start: ZonedDateTime, end: ZonedDateTime, step: TemporalAmount): Iterator[ZonedDateTime] =
    Iterator.iterate(start)(_.plus(step)).takeWhile(x => !(x.isEqual(end) || x.isAfter(end)))

  def loadCsvData[T: Encoder](s3path: String, date: LocalDate, schema: StructType): Dataset[T] = {
    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(cleansedDataPaths(s3path, date): _*)
      .selectAs[T]
  }


  def loadParquetDataHourly[T: Encoder](s3path: String, date: LocalDate, hours: Seq[Int], source: Option[String] = None): Dataset[T] = {
    val paths = parquetHourlyDataPaths(s3path, date, source, hours)
    spark.read.parquet(paths: _*)
      .selectAs[T]
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

  val vec_size: UserDefinedFunction = udf((v: Vector) => v.size)
  val vec_indices: UserDefinedFunction = udf((v: SparseVector) => v.indices)
  val vec_values: UserDefinedFunction = udf((v: SparseVector) => v.values)
}