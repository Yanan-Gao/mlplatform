package com.thetradedesk.data.transform

import com.thetradedesk.data._
import com.thetradedesk.data.schema.{CleanInputData, ModelFeature, ModelTarget}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when, xxhash64}

import java.time.LocalDate

object TrainingDataTransform {

  val modelFeatures: Array[ModelFeature] = Array(


    ModelFeature("SupplyVendor", "string", 102, 0),
    ModelFeature("DealId", "string", 5002, 0),
    ModelFeature("SupplyVendorPublisherId", "string", 15002, 0),
    ModelFeature("SupplyVendorSiteId", "string", 102, 0),
    ModelFeature("Site", "string", 350002, 0),
    ModelFeature("AdFormat", "string", 102, 0),
    ModelFeature("ImpressionPlacementId", "string", 102, 0),
    ModelFeature("Country", "string", 252, 0),
    ModelFeature("Region", "string", 4002, 0),
    ModelFeature("Metro", "string", 302, 0),
    ModelFeature("City", "string", 75002, 0),
    ModelFeature("Zip", "string", 90002, 0),
    ModelFeature("DeviceMake", "string", 1002, 0),
    ModelFeature("DeviceModel", "string", 10002, 0),
    ModelFeature("RequestLanguages", "string", 502, 0),

    // these are already integers
    ModelFeature("RenderingContext", "int", 6, 0),
    ModelFeature("UserHourOfWeek", "int", 24 * 7 + 2, 0),
    ModelFeature("AdsTxtSellerType", "int", 7, 0),
    ModelFeature("PublisherType", "int", 7, 0),
    ModelFeature("DeviceType", "int", 9, 0),
    ModelFeature("OperatingSystemFamily", "int", 10, 0),
    ModelFeature("Browser", "int", 20, 0)

  )

  val modelTargets = Vector(
    ModelTarget("is_imp", "float", nullable = false),
    ModelTarget("AuctionBidPrice", "float", nullable = false),
    ModelTarget("RealMediaCost", "float", nullable = true),
    ModelTarget("mb2w", "float", nullable = false),
    ModelTarget("FloorPriceInUSD", "float", nullable = true),
  )

  val TRAIN = "train"
  val VAL = "validation"
  val TEST = "test"

  val TFRECORD_FORMAT = "tfrecord"
  val PARQUET_FORMAT = "parquet"
  val DEFAULT_NUM_PARTITIONS = 100

  val STRING_FEATURE_TYPE = "string"
  val INT_FEATURE_TYPE = "int"

  val NUM_OUTPUT_PARTITIONS = Map(
    TRAIN -> 100,
    VAL -> 5,
    TEST -> 5
  )

  def modelTargetCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }

  def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, cardinality, _) => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, cardinality, _) => when(col(name).isNotNullOrEmpty, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
    }.toArray
  }

  def transform(s3Path: String, ttdEnv: String, inputS3Prefix: String, outputS3Prefix: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, formats: Seq[String])
               (implicit prometheus: PrometheusClient): Unit = {

    // load input data
    val paths = inputDataPaths(s3Path = s3Path, s3Prefix = inputS3Prefix, ttdEnv = ttdEnv, svName = svName, endDate = endDate, lookBack = lookBack)

    // split the data into training, validation, test
    // paths --> training_paths, val_ts_path
    val (trainPaths, valTestPaths) = temporalPathSplits(paths)

    val trainInputData = loadInputData(trainPaths)
    val valTestInputData = loadInputData(valTestPaths)
    val (valInputData, testInputData) = createDataSplits(valTestInputData)

    val trainCount = prometheus.createGauge("train_row_count", "rows of train data")
    val validationCount = prometheus.createGauge("validation_row_count", "rows of validation data")
    val testCount = prometheus.createGauge("test_row_count", "ros of test data")

    trainCount.set(trainInputData.cache.count())
    validationCount.set(valInputData.cache.count())
    testCount.set(testInputData.cache.count())

    // convert to int (hash)
    val selectionTabular = intModelFeaturesCols(modelFeatures) ++ modelTargetCols(modelTargets)


    outputPermutations(trainInputData, valInputData, testInputData, selectionTabular, formats)
      .foreach {
        case ((name, df, partitions), TFRECORD_FORMAT) =>
          writeTfRecord(
            df,
            partitions,
            outputDataPaths(s3Path, outputS3Prefix, ttdEnv, svName, endDate, lookBack.get, TFRECORD_FORMAT, name)
          )

        case ((name, df, partitions), PARQUET_FORMAT) =>
          df
            .repartition(partitions)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(
              outputDataPaths(s3Path, outputS3Prefix, ttdEnv, svName, endDate, lookBack.get, PARQUET_FORMAT, name)
            )
        case _ =>
      }
  }

  def outputPermutations(trainInputData: Dataset[CleanInputData], valInputData: Dataset[CleanInputData], testInputData: Dataset[CleanInputData], selectQuery: Array[Column], formats: Seq[String]): Seq[((String, DataFrame, Int), String)] = {
    Seq(
      (TRAIN, trainInputData.select(selectQuery: _*), NUM_OUTPUT_PARTITIONS.getOrElse(TRAIN, DEFAULT_NUM_PARTITIONS)),
      (VAL, valInputData.select(selectQuery: _*), NUM_OUTPUT_PARTITIONS.getOrElse(VAL, DEFAULT_NUM_PARTITIONS)),
      (TEST, testInputData.select(selectQuery: _*), NUM_OUTPUT_PARTITIONS.getOrElse(TEST, DEFAULT_NUM_PARTITIONS))
    ).flatMap(
      x => formats.map(
        y => (x, y))
    )
  }

  def loadInputData(paths: Seq[String])(implicit spark: SparkSession): Dataset[CleanInputData] = {
    import spark.implicits._
    spark.read.parquet(paths: _*).selectAs[CleanInputData]
  }

  def inputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None): Seq[String] = {
    plutusDataPaths(s3Path, ttdEnv, s3Prefix, svName, endDate, lookBack)
  }

  def outputDataPaths(s3Path: String, s3Prefix: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, lookBack: Int, dataFormat: String, splitName: String): String = {
    f"${plutusDataPath(s3Path, ttdEnv, s3Prefix, svName, endDate)}/lookback=$lookBack/format=$dataFormat/$splitName/"
  }

  def writeTfRecord(df: DataFrame, partitions: Int, outputPath: String): Unit = {
    df
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(outputPath)
  }

  def temporalPathSplits(paths: Seq[String]): (Seq[String], Seq[String]) = {
    assert(paths.size >= 3, "cannot create temporal splits for less than 3 days of data")
    val p = paths.sorted.reverse

    val testAndVal = p.head
    val train = p.tail
    (train, Seq(testAndVal))
  }

  def createDataSplits(ds: Dataset[CleanInputData], splits: Option[Array[Double]] = Some(Array(0.5, 0.5)), seedValue: Option[Long] = Some(42L)): (Dataset[CleanInputData], Dataset[CleanInputData]) = {
    val s = ds.randomSplit(splits.get, seed = seedValue.get)
    (s(0), s(1))
  }
}
