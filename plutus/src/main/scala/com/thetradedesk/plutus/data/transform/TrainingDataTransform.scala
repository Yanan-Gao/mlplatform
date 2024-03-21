package com.thetradedesk.plutus.data.transform

import job.ModelInputProcessor.{numCsvPartitions, onlyWriteMaxIntMod, onlyWriteSingleDay, outputPath, prometheus}
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, loadModelFeatures}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{coalesce, col}

import java.time.LocalDate
import com.thetradedesk.plutus.data.schema.{CleanInputData, MetaData, ModelTarget}
import com.thetradedesk.plutus.data.{DEFAULT_SHIFT, plutusDataPath, plutusDataPaths, hashedModMaxIntFeaturesCols}

object TrainingDataTransform {
  val STRING_FEATURE_TYPE = "string"
  val INT_FEATURE_TYPE = "int"
  val FLOAT_FEATURE_TYPE = "float"

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

  val NUM_OUTPUT_PARTITIONS = Map(
    TRAIN -> 100,
    VAL -> 5,
    TEST -> 5
  )

  def modelTargetCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }


  val trainCount = prometheus.createGauge("train_row_count", "rows of train data", labelNames = "ssp")
  val validationCount = prometheus.createGauge("validation_row_count", "rows of validation data", labelNames = "ssp")
  val testCount = prometheus.createGauge("test_row_count", "ros of test data", labelNames = "ssp")

  def transform(s3Path: String, ttdEnv: String, inputS3Prefix: String, outputS3Prefix: String, svName: Option[String], endDate: LocalDate, lookBack: Option[Int] = None, formats: Seq[String], featuresJson: String): Unit = {

    // load input data
    val paths = inputDataPaths(s3Path = s3Path, s3Prefix = inputS3Prefix, ttdEnv = ttdEnv, svName = svName, endDate = endDate, lookBack = lookBack)

    val modelFeatures = loadModelFeatures(featuresJson)

  /*  metaData.write.option("header", "true")
      .option("delimiter","\t")
      .mode(SaveMode.Overwrite)
      .csv(metaDataS3)
    */
    
    // convert to int (hash)
    val selectionTabular = intModelFeaturesCols(modelFeatures) ++ modelTargetCols(modelTargets)
    val selectionTabMaxMod = hashedModMaxIntFeaturesCols(modelFeatures, DEFAULT_SHIFT) ++ modelTargetCols(modelTargets)

    // single day
    val singleDay = loadInputData(Seq(paths.head))

    if (!onlyWriteMaxIntMod) {
      writeSingleDayProcessed(s3Path, ttdEnv, svName, endDate, singleDay, selectionTabular, "singledayprocessed")
    }
    writeSingleDayProcessed(s3Path, ttdEnv, svName, endDate, singleDay, selectionTabMaxMod, "singledayprocessedmaxint")


    if (!onlyWriteSingleDay) {

      // split the data into training, validation, test
      // paths --> training_paths, val_ts_path
      val (trainPaths, valTestPaths) = temporalPathSplits(paths)

      val trainInputData = loadInputData(trainPaths)
      val valTestInputData = loadInputData(valTestPaths)
      val (valInputData, testInputData) = createDataSplits(valTestInputData)

      val trainRows = trainInputData.cache.count()
      val testRows = testInputData.cache.count()
      val valRows = valInputData.cache.count()

      val metaData: Dataset[MetaData] = Seq(MetaData(endDate, trainRows, testRows, valRows)).toDS()
      val metaDataS3 = plutusDataPath(s3Path, ttdEnv, prefix = "metadata", svName, endDate)

      trainCount.labels(svName.getOrElse("none")).set(trainRows)
      validationCount.labels(svName.getOrElse("none")).set(testRows)
      testCount.labels(svName.getOrElse("none")).set(valRows)


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

  def writeSingleDayProcessed(s3Path: String, ttdEnv: String, svName: Option[String], endDate: LocalDate, data: Dataset[CleanInputData], selectionTab: Array[Column], prefix: String) = {
    data.select(selectionTab: _*)
      .coalesce(numCsvPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(plutusDataPath(s3Path, ttdEnv, prefix = prefix, svName, endDate))
  }

  def loadInputData(paths: Seq[String]): Dataset[CleanInputData] = {
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
