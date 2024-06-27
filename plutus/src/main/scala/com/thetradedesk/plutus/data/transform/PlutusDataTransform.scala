package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.shared.loadModelFeatures
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

object PlutusDataTransform extends Logger {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2
  val DEFAULT_IMPLICIT_STRATIFICATION = 0.1
  val DEFAULT_MIN_BID = 0.01
  val DEFAULT_MAX_BID = 300.0
  val LAT_LONG_PRECISION = 4
  val DEFAULT_FACET_PARTITIONS = 50


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
                      inputTtdEnv: String,
                      maybeOutputTtdEnv: Option[String] = None,
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val mergedDataset: Dataset[PcResultsMergedDataset] = loadParquetData[PcResultsMergedDataset](PcResultsMergedDataset.S3_PATH(Some(inputTtdEnv)), date)

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
      .drop("AdWidthInPixels", "AdHeightInPixels")
      .repartition(partitions)
      .persist(StorageLevel.MEMORY_ONLY_2)

    val bidsGaugeExplicit = prometheus.createGauge("clean_explicit_v2", "count of clean explicit dataset v2")
    bidsGaugeExplicit.set(cleanExplicitDataset.count())

    val mbtwSupplyVendors = cleanExplicitDataset.select("SupplyVendor").distinct().as[String].collect().toSeq

    mbtwSupplyVendors.par.foreach {
      sv =>
        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/sv/$sv/${cleanOutputPrefix}/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(hashedModMaxIntCols(cleanExplicitDataset.dtypes, DEFAULT_SHIFT, PcResultsMergedDataset.NON_FEATURE_STRINGS, PcResultsMergedDataset.NON_SHIFT_INTS): _*)
          .na.fill(0)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/sv/$sv/${cleanOutputPrefix}-hashed-mod-maxint/${explicitDatePart(date)}")
    }

    val channelTypes = cleanExplicitDataset.select("ChannelSimple").distinct().as[String].collect().toSeq
    channelTypes.par.foreach {
      ch =>
        cleanExplicitDataset
          .filter(col("Channel") === ch)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/ch/$ch/${cleanOutputPrefix}/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("Channel") === ch)
          .select(hashedModMaxIntCols(cleanExplicitDataset.dtypes, DEFAULT_SHIFT, PcResultsMergedDataset.NON_FEATURE_STRINGS, PcResultsMergedDataset.NON_SHIFT_INTS): _*)
          .na.fill(0)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/ch/$ch/${cleanOutputPrefix}-hashed-mod-maxint/${explicitDatePart(date)}")
    }
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
                      inputTtdEnv: String,
                      maybeOutputTtdEnv: Option[String] = None,
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {


    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val mergedDataset: Dataset[PcResultsMergedDataset] = loadParquetData[PcResultsMergedDataset](PcResultsMergedDataset.S3_PATH(Some(inputTtdEnv)), date)


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
      .repartition(partitions)
      .persist()
    val bidsGaugeImplicit = prometheus.createGauge("clean_implicit_v2", "count of clean explicit dataset v2")
    bidsGaugeImplicit.set(cleanImplicitDataset.count())

    cleanImplicitDataset
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(hashedModMaxIntCols(cleanImplicitDataset.dtypes, DEFAULT_SHIFT, PcResultsMergedDataset.NON_FEATURE_STRINGS, PcResultsMergedDataset.NON_SHIFT_INTS): _*)
      .na.fill(0)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}-hashed-mod-maxint/${explicitDatePart(date)}")

    val channelTypes = cleanImplicitDataset.select("ChannelSimple").distinct().as[String].collect().toSeq
    channelTypes.par.foreach {
      ch =>
        cleanImplicitDataset
          .filter(col("Channel") === ch)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/ch/$ch/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}/${explicitDatePart(date)}")

        cleanImplicitDataset
          .filter(col("Channel") === ch)
          .select(hashedModMaxIntCols(cleanImplicitDataset.dtypes, DEFAULT_SHIFT, PcResultsMergedDataset.NON_FEATURE_STRINGS, PcResultsMergedDataset.NON_SHIFT_INTS): _*)
          .na.fill(0)
          .repartition(maybeFacetPartitions.getOrElse(DEFAULT_FACET_PARTITIONS))
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/ch/$ch/sample=${(implicitSampleRate * 100).intValue()}/${cleanOutputPrefix}-hashed-mod-maxint/${explicitDatePart(date)}")
    }
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