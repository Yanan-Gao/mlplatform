package com.thetradedesk.plutus.data.transform


import com.thetradedesk.plutus.data.{RenderingContext, envForReadInternal, envForWrite, plutusDataPath}
import job.CleanInputDataProcessor.prometheus
import com.thetradedesk.plutus.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions.{col, lit, round, when}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDate

object CleanInputDataTransform {

  val DEFAULT_NUM_PARTITION = 200

  val totalData = prometheus.createGauge("clean_data_rows", "count of processed rows", labelNames = "ssp")
  val mbwDataCount = prometheus.createGauge("clean_data_rows_mb2w", "Total Data with MB2W", labelNames = "ssp")
  val mbwBidsCount = prometheus.createGauge("clean_data_bids_mb2w", "Total Bids with MB2W", labelNames = "ssp")
  val mbwImpsCount = prometheus.createGauge("clean_data_impressions_mb2w", "Total Imps with MB2W", labelNames = "ssp")
  val mbwValidBidsCount = prometheus.createGauge("clean_data_bids_valid_mb2w", "Total Bids with Valid MB2W", labelNames = "ssp")
  val mbwValidImpsCount = prometheus.createGauge("clean_data_impressions_valid_mb2w", "Total Impressions with Valid MB2W", labelNames = "ssp")


  def transform(date: LocalDate, rawInputS3Path: String, rawInputPrefix: String, extremeValueThreshold: Double, svName: Option[String] = None, cleanOutputS3Path: String, cleanOutputPrefix: String): Unit = {
    // only reading the data from one ssp at a time so no need to filter for supply vendor here or in createCleanDataset
    val inputPath = plutusDataPath(rawInputS3Path, envForReadInternal, rawInputPrefix, svName, date)

    val cleanData = createCleanDataset(inputPath, extremeValueThreshold)

    totalData.labels(svName.getOrElse("none")).set(cleanData.cache().count)
    mbwDataCount.labels(svName.getOrElse("none")).set(cleanData.filter(col("mb2w").isNotNull).count)
    mbwBidsCount.labels(svName.getOrElse("none")).set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNull).count)
    mbwImpsCount.labels(svName.getOrElse("none")).set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNotNull).count)

    // clean bids t
    mbwValidBidsCount.labels(svName.getOrElse("none")).set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNull).filter(col("mb2w") >= col("b_RealBidPrice")).count)

    mbwValidImpsCount.labels(svName.getOrElse("none")).set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNotNull).filter(col("mb2w") <= col("RealMediaCost")).count)

    val outputPath = plutusDataPath(cleanOutputS3Path, envForWrite, cleanOutputPrefix, svName, date)
    outputCleanData(cleanData, outputPath)
  }


  def outputCleanData(dataset: Dataset[CleanInputData], outputPath: String, numParitions: Option[Int] = None): Unit = {
    dataset
      .repartition(numParitions.getOrElse(DEFAULT_NUM_PARTITION))
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }


  def createCleanDataset(s3Path: String, extremeValueThreshold: Double): Dataset[CleanInputData] = {

    val df = spark.read.parquet(s3Path)
      //python code expecting 'is_imp' target, but doesnt fit with column name style before this point, so changing name here
      .withColumn("is_imp", col("IsImp").cast(FloatType))
    //TODO: counters on size etc here -> put counter on the output as this input should be captured in the raw data creation counters

    val ds = df
      .filter(col("mb2w").isNotNull)
      .withColumn("valid",
        // there are cases that dont make sense - we choose to remove these for simplicity while we investigate further.
        // impressions where MB2W < Media Cost and MB2W is above a floor (if present)
        when(
          (col("is_imp") === 1.0) &&
            (col("mb2w") <= col("RealMediaCost")) &&
            ((col("mb2w") >= col("FloorPriceInUSD")) || col("FloorPriceInUSD").isNullOrEmpty), true)

          // Bids where MB2W is > bid price AND not an extreme value AND is above floor (if present)
          .when(
            (col("is_imp") === 0.0) &&
              (col("mb2w") > col("b_RealBidPrice")) &&
              (col("FloorPriceInUSD").isNullOrEmpty || (col("mb2w") >= col("FloorPriceInUSD"))) &&
              (round(col("b_RealBidPrice") / col("mb2w"), 1) > extremeValueThreshold) &&
              ((col("mb2w") > col("FloorPriceInUSD")) || (col("FloorPriceInUSD").isNullOrEmpty)), true)
          .otherwise(false)
      )
      // We only want to use location data from InApp Rendering Context. Rest is not good data.
      .withColumn("Latitude", when(col("RenderingContext") === lit(RenderingContext.InApp), col("Latitude")).otherwise(lit(0.0f)))
      .withColumn("Longitude", when(col("RenderingContext") === lit(RenderingContext.InApp), col("Longitude")).otherwise(lit(0.0f)))
      .withColumn("AuctionBidPrice",
        when(col("RealMediaCost").isNotNull, col("RealMediaCost"))
          .when((col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD"))), col("FloorPriceInUSD"))
          .otherwise(col("b_RealBidPrice")))
      .filter(col("valid") === true)
      .drop("valid")

    ds.selectAs[CleanInputData]
  }
}
