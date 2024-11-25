package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.shared.loadModelFeatures
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.utils.ParallelParquetWriter
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext

object PlutusDataTransform extends Logger {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2
  val DEFAULT_IMPLICIT_STRATIFICATION = 0.1
  val DEFAULT_MIN_BID = 0.01
  val DEFAULT_MAX_BID = 300.0
  val LAT_LONG_PRECISION = 4
  val DEFAULT_FACET_PARTITIONS = 50

  val COL_NAME_CHANNEL = "ch"
  val COL_NAME_SUPPLYVENDOR = "sv"

  val NON_SHIFT_INTS: Seq[String] = Seq("PredictiveClearingMode", "BidBelowFloorExceptedSource", "Strategy", "LossReason", "UserSegmentCount")
  val NON_FEATURE_STRINGS: Seq[String] = Seq("BidRequestId", "UIID", "Model", COL_NAME_SUPPLYVENDOR, COL_NAME_CHANNEL)


  def EncodeLatLong(df: Dataset[PcResultsMergedDataset]): DataFrame = {
    //    val R = lit(6371)
    val R = lit(1)

    df
      .withColumn(
        "latlong_x",
        when(
          col("Latitude").isNotNull && col("Longitude").isNotNull,
          round(
            R * cos(radians(col("Latitude"))) * cos(radians(col("Longitude"))),
            LAT_LONG_PRECISION
          )
        ).otherwise(0)
      )
      .withColumn(
        "latlong_y",
        when(
          col("Latitude").isNotNull && col("Longitude").isNotNull,
          round(
            R * cos(radians(col("Latitude"))) * sin(radians(col("Longitude"))),
            LAT_LONG_PRECISION
          )
        ).otherwise(0)
      )
      .withColumn(
        "latlong_z",
        when(
          col("Latitude").isNotNull,
          round(
            R * sin(radians(col("Latitude"))),
            LAT_LONG_PRECISION
          )
        ).otherwise(0)
      )
  }


  def processExplicit(date: LocalDate,
                      partitions: Int,
                      maybeFacetPartitions: Option[Int] = None,
                      outputPath: String,
                      dataVersion: Int,
                      cleanOutputPrefix: String,
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val datePrefix = explicitDatePart(date)
    val mergedDataset: Dataset[PcResultsMergedDataset] = loadParquetData[PcResultsMergedDataset](PcResultsMergedDataset.S3_PATH(Some(envForRead)), date)
      .filter(! (col("IsImp") === false && col("BidBelowFloorExceptedSource") =!= 0)) // remove BBF bids (keep the impressions)
      // We only want to use location data from InApp Rendering Context. Rest is not good data.
      .withColumn("Latitude", when(col("RenderingContext") === lit(RenderingContext.InApp), col("Latitude")).otherwise(lit(0.0f)))
      .withColumn("Longitude", when(col("RenderingContext") === lit(RenderingContext.InApp), col("Longitude")).otherwise(lit(0.0f)))
      .as[PcResultsMergedDataset]

    // should just check that the data contains these rather than filtering to this list
    val modelFeatures = loadModelFeatures(featuresJson)
    println(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES))
    println(checkColumnCoverage(modelFeatures, mergedDataset.columns))
    //    assert(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES), "Feature Cols not present in Data Cols")
    //    assert(checkColumnCoverage(modelFeatures, rawExplicitData.columns), s"Missing Feature Columns in Data ${modelFeatures.map(_.name)} ${rawExplicitData.columns}")

    val cleanExplicitDataset = EncodeLatLong(mergedDataset)
      .filter(col("AuctionType").isin(AuctionType.FirstPrice))
      .filter(col("isMbtwValidStrict"))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .withColumn(COL_NAME_CHANNEL, lower(regexp_replace(col("Channel"), " ", "")))
      .withColumn(COL_NAME_SUPPLYVENDOR, col("SupplyVendor"))
      .drop("AdWidthInPixels", "AdHeightInPixels")
      .repartition(partitions)
      .persist(StorageLevel.MEMORY_ONLY_2)

    val cleanExplicitHashModMaxDataset = cleanExplicitDataset
      .select(hashedModMaxIntCols(cleanExplicitDataset.dtypes, DEFAULT_SHIFT, NON_FEATURE_STRINGS ++ NON_SHIFT_INTS): _*)
      .na.fill(0)

    val bidsGaugeExplicit = prometheus.createGauge("plutus_clean_explicit_v2", "count of clean explicit dataset v2")
    bidsGaugeExplicit.set(cleanExplicitDataset.count())

    val writer = new ParallelParquetWriter()

    writer.EnqueueWrite(
      writer =
        cleanExplicitDataset
          .drop(COL_NAME_CHANNEL)
          .write
          .partitionBy(COL_NAME_SUPPLYVENDOR)
          .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/explicit/$cleanOutputPrefix/$datePrefix/sv"
    )

    writer.EnqueueWrite(
      writer =
        cleanExplicitHashModMaxDataset
          .drop(COL_NAME_CHANNEL)
          .write
          .partitionBy(COL_NAME_SUPPLYVENDOR)
          .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/explicit/$cleanOutputPrefix-hashed-mod-maxint/$datePrefix/sv"
    )

    writer.EnqueueWrite(
      writer =
        cleanExplicitDataset
          .drop(COL_NAME_SUPPLYVENDOR)
          .write
          .partitionBy(COL_NAME_CHANNEL)
          .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/explicit/$cleanOutputPrefix/$datePrefix/ch"
    )

    writer.EnqueueWrite(
      writer =
        cleanExplicitHashModMaxDataset
          .drop(COL_NAME_SUPPLYVENDOR)
          .write
          .partitionBy(COL_NAME_CHANNEL)
          .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/explicit/$cleanOutputPrefix-hashed-mod-maxint/$datePrefix/ch"
    )

    writer.WriteAll()
    cleanExplicitDataset.unpersist()
  }

  def checkColumnCoverage(modelCols: Seq[ModelFeature], dataCols: Array[String]): Seq[String] = {
    modelCols.map(_.name.toLowerCase).diff(dataCols.map(_.toLowerCase))
    //    modelCols.map(c => c.name).map(c => dataCols.contains(c)).forall(_ == true)
  }

  def checkFeatureCoverage(modelCols: Seq[ModelFeature], dataCols: Seq[ModelFeature]): Seq[String] = {
    dataCols.map(_.name.toLowerCase).diff(modelCols.map(_.name.toLowerCase))
    //    modelCols.map(_.name).map(dataCols.map(_.name).contains(_)).forall(_ == true)
  }


  def processImplicit(date: LocalDate,
                      partitions: Int,
                      maybeFacetPartitions: Option[Int] = None,
                      outputPath: String,
                      dataVersion: Int,
                      implicitSampleRate: Double,
                      cleanOutputPrefix: String,
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {


    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val mergedDataset: Dataset[PcResultsMergedDataset] = loadParquetData[PcResultsMergedDataset](PcResultsMergedDataset.S3_PATH(Some(envForRead)), date)
    val datePrefix = explicitDatePart(date)

    // should just check that the data contains these rather than filtering to this list
    val modelFeatures = loadModelFeatures(featuresJson)
    println(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES))
    println(checkColumnCoverage(modelFeatures, mergedDataset.columns))
    //    assert(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES), "Feature Cols not present in Data Cols")
    //    assert(checkColumnCoverage(modelFeatures, rawImplicitData.columns), s"Missing Feature Columns in Data ${modelFeatures.map(_.name)} ${rawImplicitData.columns}")

    val cleanImplicitDataset = EncodeLatLong(
      cleanImplicitData(
        mergedDataset = mergedDataset,
        samplingRate = Some(implicitSampleRate)
      )
    ).withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .drop("AdWidthInPixels", "AdHeightInPixels")
      .withColumn(COL_NAME_CHANNEL, lower(regexp_replace(col("Channel"), " ", "")))
      .repartition(partitions)
      .persist()

    val cleanImplicitHashModMaxDataset = cleanImplicitDataset
      .select(hashedModMaxIntCols(cleanImplicitDataset.dtypes, DEFAULT_SHIFT, NON_FEATURE_STRINGS ++ NON_SHIFT_INTS): _*)
      .na.fill(0)

    val bidsGaugeImplicit = prometheus.createGauge("plutus_clean_implicit_v2", "count of clean explicit dataset v2")
    bidsGaugeImplicit.set(cleanImplicitDataset.count())

    val writer = new ParallelParquetWriter()

    // Writing all hash mod max rows
    writer.EnqueueWrite(
      writer = cleanImplicitDataset
        .drop(COL_NAME_CHANNEL)
        .write
        .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/${envForWrite}/implicit/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}/$datePrefix/all"
    )

    // Writing all hash mod max rows
    writer.EnqueueWrite(
      writer = cleanImplicitHashModMaxDataset
        .drop(COL_NAME_CHANNEL)
        .write
        .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/${envForWrite}/implicit/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}-hashed-mod-maxint/$datePrefix/all"
    )

    // Separating by channel
    writer.EnqueueWrite(
      writer = cleanImplicitDataset
        .write
        .partitionBy(COL_NAME_CHANNEL)
        .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/implicit/sample=${(implicitSampleRate * 100).intValue()}/$cleanOutputPrefix/$datePrefix/ch"
    )

    // Writing all hash mod max rows separated by channel
    writer.EnqueueWrite(
      writer = cleanImplicitHashModMaxDataset
        .write
        .partitionBy(COL_NAME_CHANNEL)
        .mode(SaveMode.Overwrite),
      path = s"$versionedOutputPath/$envForWrite/implicit/sample=${(implicitSampleRate * 100).intValue()}/$cleanOutputPrefix-hashed-mod-maxint/$datePrefix/ch"
    )

    writer.WriteAll()
    cleanImplicitDataset.unpersist()
  }


  def cleanImplicitData(mergedDataset: Dataset[PcResultsMergedDataset], samplingRate: Option[Double] = None, randomSeed: Int = 123): Dataset[PcResultsMergedDataset] = {
    stratification(
      validPlatformCPM(mergedDataset),
      sampleRate = samplingRate.getOrElse(DEFAULT_IMPLICIT_STRATIFICATION),
      randomSeed = randomSeed
    ).filter(col("AuctionType").isin(AuctionType.FirstPrice))
  }

  def validPlatformCPM(rawDs: Dataset[PcResultsMergedDataset], maybeMaxBid: Option[Double] = None, maybeMinBid: Option[Double] = None): Dataset[PcResultsMergedDataset] = {
    rawDs.filter(
      (col("FinalBidPrice") < maybeMaxBid.getOrElse(DEFAULT_MAX_BID))
        && (col("FinalBidPrice") > maybeMinBid.getOrElse(DEFAULT_MIN_BID))
    )
  }

  def stratification(mergedDataset: Dataset[PcResultsMergedDataset],
                     sampleRate: Double,
                     randomSeed: Int = 123,
                     stratificationColumns: Seq[Column] = Seq(col("SupplyVendor"), col("IsImp")),
                     stratificationColumnName: String = "stratify_col"): Dataset[PcResultsMergedDataset] = {

    if (sampleRate == 1.0) {
      mergedDataset
    }
    else {
      val implicitData = mergedDataset.withColumn(
        stratificationColumnName,
        concat_ws("_", stratificationColumns: _*)
      )

      val fractions = implicitData.select(
        stratificationColumnName
      ).distinct().as[String].collect().map((_, sampleRate)).toMap

      implicitData.stat.sampleBy(
        stratificationColumnName, fractions, randomSeed
      ).drop(stratificationColumnName).as[PcResultsMergedDataset]
    }
  }
}