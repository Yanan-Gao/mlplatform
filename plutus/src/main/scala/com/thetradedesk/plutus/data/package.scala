package com.thetradedesk.plutus

import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE}
import com.thetradedesk.plutus.data.schema.ModelTarget
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Encoder}

import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAmount
import java.time.{LocalDate, LocalDateTime}
import java.util.UUID
import java.util.regex.Pattern
import scala.annotation.nowarn
import scala.util.Try

package object data {

  val DATA_VERSION = 3

  val PLUTUS_DATA_SOURCE = "plutus"
  val IMPLICIT_DATA_SOURCE = "bidsimpressions"

  val DEFAULT_SV_NAME = "google"
  val DEFAULT_IMPLICIT_PREFIX = "bidsimpressions"

  val MISSING_DATA_VALUE: Int = 0

  //  Reserve Spaces for values
  //  NOTE: this is a breaking change as previously only 0 was reserved
  val DEFAULT_SHIFT = 3

  def hashedModMaxIntFeaturesCols(inputColAndDims: Seq[ModelFeature], shift: Int): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _, _) => when(col(name).isNotNullOrEmpty, shiftModMaxValueUDF(xxhash64(col(name)), lit(shift))).otherwise(MISSING_DATA_VALUE).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, _, _, _) => when(col(name).isNotNull, shiftModMaxValueUDF(col(name), lit(shift))).otherwise(MISSING_DATA_VALUE).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _, _) => col(name).alias(name)
    }.toArray
  }

  def hashedModMaxIntCols(datasetDtype: Array[(String, String)], shift: Int, excludeCols: Seq[String] = Seq()): Array[Column] = {
    datasetDtype.map {
      case (name, "StringType") if !excludeCols.contains(name) => when(col(name).isNotNullOrEmpty, shiftModMaxValueUDF(xxhash64(col(name)), lit(shift))).otherwise(MISSING_DATA_VALUE).alias(name)
      case (name, "IntegerType") if !excludeCols.contains(name) => when(col(name).isNotNull, shiftModMaxValueUDF(col(name), lit(shift))).otherwise(MISSING_DATA_VALUE).alias(name)
      case (name, _) => col(name).alias(name)
    }.toArray
  }

  // Duplicated by com.thetradedesk.geronimo.shared.shiftMod
  def shiftMod(hashValue: Long, cardinality: Int, shift: Int = 1): Int = {
    val modulo = math.min(cardinality - shift, Int.MaxValue - shift)
    val index = (hashValue % modulo).toInt

    // Adjusting shift if index is negative
    val adjustedShift = if (index < 0) shift + modulo else shift

    index + adjustedShift
  }

  def modelTargeCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }

  def modelFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(t => col(t.name).alias(t.name)).toArray
  }

  def shiftModMaxValueUDF: UserDefinedFunction = udf((hashValue: Long, shift: Int) => {
    shiftMod(hashValue, Int.MaxValue, shift)
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
    shiftMod(hashValue, (Int.MaxValue))
  })

  /// We want the stage stat to be > prodstat - margin.
  def isOkay(prodStat: Double, stageStat: Double, margin: Double = 0.05) = stageStat > (1 - margin) * prodStat

  /// We want the stage stat to be >= prodstat.
  def isBetter(prodStat: Double, stageStat: Double) = stageStat >= prodStat


  def paddedDatePart(date: LocalDate, separator: Option[String] = None): String = {
    separator match {
      case Some(s) => f"${date.getYear}$s${date.getMonthValue}%02d$s${date.getDayOfMonth}%02d"
      case _ => f"${date.getYear}${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
    }
  }

  def paddedDateTimePart(dateTime: LocalDateTime, separator: Option[String] = None): String = {
    f"date=${paddedDatePart(dateTime.toLocalDate, separator)}/hour=${dateTime.getHour}"
  }

  def explicitDateTimePart(date: LocalDateTime): String = {
    f"year=${date.getYear}/month=${date.getMonthValue}%02d/day=${date.getDayOfMonth}%02d/hourPart=${date.getHour}"
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

  def parquetHourlyDataPaths(s3path: String, dateTime: LocalDateTime, source: Option[String] = None, lookBack: Option[Int] = None, sep: Option[String] = None): Seq[String] = {
    source match {
      case Some(PLUTUS_DATA_SOURCE | IMPLICIT_DATA_SOURCE) =>
        (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${explicitDateTimePart(dateTime.minusHours(i))}")
      case None =>
        (0 to lookBack.getOrElse(0)).map(i => f"$s3path/${paddedDateTimePart(dateTime.minusHours(i))}")
    }
  }

  def cleansedDataPaths(basePath: String, date: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"${basePath}/${paddedDatePart(date.minusDays(i), separator = Some("/"))}/*/*/*.gz")
  }

  /**
   * @param lookBack Default is None (equivalent to 0), which returns 1 path exactly at dateTime.
   *                 If lookback > 0, function will return (lookback + 1) paths.
   */
  def generateDataPathsHourly(basePath: String, extGenerator: LocalDateTime => String, dateTime: LocalDateTime, lookBack: Option[Int] = None): Seq[String] = {
    (0 to lookBack.getOrElse(0)).map(i => f"$basePath${extGenerator(dateTime.minusHours(i))}")
  }


  /**
   * @param lookBack Default is None (equivalent to 0), which returns 1 path exactly at dateTime.
   *                 If lookback > 0, function will return (lookback + 1) paths.
   */
  def generateDataPathsDaily(basePath: String, extGenerator: LocalDate => String, date: LocalDate, lookBack: Int = 0): Seq[String] = {
    (0 to lookBack).map(i => f"$basePath${extGenerator(date.minusDays(i))}")
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

  def loadParquetData[T: Encoder](s3path: String, date: LocalDate, source: Option[String] = None, lookBack: Option[Int] = None, nullIfColAbsent: Boolean = false): Dataset[T] = {
    val paths = parquetDataPaths(s3path, date, source, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T](nullIfColAbsent)
  }

  def dateRange(start: LocalDateTime, end: LocalDateTime, step: TemporalAmount): Iterator[LocalDateTime] =
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

  def loadCsvData[T: Encoder](basePath: String, extGenerator: LocalDateTime => String, dateTime: LocalDateTime, schema: StructType): Dataset[T] = {
    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(generateDataPathsHourly(basePath, extGenerator, dateTime): _*)
      .selectAs[T]
  }

  def loadParquetDataHourly[T: Encoder](s3path: String, dateTime: LocalDateTime, lookBack: Option[Int] = None, source: Option[String] = None): Dataset[T] = {
    val paths = parquetHourlyDataPaths(s3path, dateTime, source, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  def loadParquetDataHourlyV2[T: Encoder](basePath: String, extGenerator: LocalDateTime => String, dateTime: LocalDateTime, lookBack: Option[Int] = None): Dataset[T] = {
    val paths = generateDataPathsHourly(basePath, extGenerator, dateTime, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T]
  }

  @deprecated("Use the Dataset classes instead")
  def loadParquetDataDailyV2[T: Encoder](basePath: String, extGenerator: LocalDate => String, date: LocalDate, lookBack: Int = 0, nullIfColAbsent: Boolean = false): Dataset[T] = {
    val paths = generateDataPathsDaily(basePath, extGenerator, date, lookBack)
    spark.read.parquet(paths: _*)
      .selectAs[T](nullIfColAbsent)
  }

  def extractDateFromPath(path: String): Option[LocalDate] = {
    val datePattern = Pattern.compile("date=(\\d{8})")
    val matcher = datePattern.matcher(path)
    if (matcher.find()) {
      val dateString = matcher.group(1)
      Try(LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyyMMdd"))).toOption
    } else {
      None
    }
  }

  def getMaxDate(paths: Seq[String], maxDate: LocalDate): Option[LocalDate] = {
    // get max date to read latest date partition from the date
    val dates = paths.flatMap(extractDateFromPath)
    dates.filter(date => date.isBefore(maxDate) || date.isEqual(maxDate))
      .reduceOption((d1: LocalDate, d2: LocalDate) => if (d1.isAfter(d2)) d1 else d2)
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

  // Copied from TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/Finance/MarketType.cs
  object MarketType extends Enumeration {
    val Unknown = "Unknown"
    val OpenMarket = "Open Market"
    val UnknownPMP = "Unknown PMP"
    val AccuenFixedPrice = "Accuen Fixed Price"
    val DirectTagNonGuaranteedFixedPrice = "Direct Tag Non Guaranteed Fixed Price"
    val DirectTagGuaranteed = "Direct Tag Guaranteed"
    val PrivateAuctionVariablePrice = "Private Auction Variable Price"
    val ProgrammaticGuaranteed = "Programmatic Guaranteed"
    val PrivateAuctionFixedPrice = "Private Auction Fixed Price"

    type MarketType = String
  }

  // Copied from TTD/Domain/Shared/TTD.Domain.Shared.PlatformModels/PlatformModels/ChannelType.cs
  object ChannelType extends Enumeration {
    val Unknown = "Unknown"
    val MobileVideoStandard = "Mobile Video Standard"
    val ConnectedTV = "Connected TV"
    val MobileVideoInApp = "Mobile Video InApp"
    val Video = "Video"
    val MobileVideoOptimizedWeb = "Mobile Video Optimized Web"
    val MobileStandardWeb = "Mobile Standard Web"
    val MobileInApp = "Mobile InApp"
    val DigitalOutOfHome = "Digital Out Of Home"
    val Display = "Display"
    val MobileOptimizedWeb = "Mobile Optimized Web"
    val NativeVideo = "Native Video"
    var Native = "Native"
    val Audio = "Audio"

    type ChannelType = String
  }

  // Copied from TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/Creatives/MediaType.cs
  object MediaTypeId extends Enumeration {
    val Unknown = 0
    val Display = 1
    val Video = 2
    val Native = 3
    val Audio = 4
    val NativeVideo = 5

    type MediaTypeId = Int
  }

  // Copied from C:\dev\adplatform\TTD\DB\Provisioning\TTD.DB.Provisioning.Primitives\Bidding\DeviceType.cs
  object DeviceTypeId extends Enumeration {
    val Unknown = 0
    val Other = 1
    val PC = 2
    val Tablet = 3
    val Mobile = 4
    val Roku = 5
    val ConnectedTV = 6
    val OutOfHome = 7
    val HomeAssistant = 8

    type DeviceType = Int
  }

  // Copied from TTD/Domain/Shared/TTD.Domain.Shared/PlatformModels/PublisherRelationshipType.cs
  object PublisherRelationshipType extends Enumeration {
    val Unknown = 0
    val Indirect = 1
    val Direct = 2
    // Deprecated: 2022-01-11
    val IndirectFixedPrice = 3

    type PublisherRelationshipType = Int
  }

  // Copied from TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/Bidding/RenderingContext.cs
  object RenderingContext extends Enumeration {
    val Other = 0 // All Web
    val InApp = 1
    val MobileOptimizedWeb = 2

    type RenderingContext = Int
  }

  // Copied from TTD/Domain/Bidding/TTD.Domain.Bidding.Public.RTB/Enums.cs
  object AuctionType extends Enumeration {
    val UnDeclared = 0
    val FirstPrice = 1
    val SecondPrice = 2
    val FixedPrice = 3

    type AuctionType = Int
  }

  // Selected codes from https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/LossReason.cs
  object LossReason extends Enumeration {
    val Win = -1
    val BidBelowAuctionFloor = 100
    val BidLostHigherBid = 102
    val ConditionalWin = 301
  }

  var envForRead: String = config.getStringOption("ttd.env").map(_.toLowerCase) match {
    case Some("prod") | Some("production") => "prod"
    case Some("prodtest") | Some("prodtesting") => "prod"
    case _ => "test"
  }

  var envForWrite: String = config.getStringOption("ttd.env").map(_.toLowerCase) match {
    case Some("prod") | Some("production") => "prod"
    case _ => "test"
  }

  // Setting this to `envForRead` will isolate each task in the chain so each task reads
  // upstream data from prod instead of test locations.
  var envForReadInternal: String = envForRead

}