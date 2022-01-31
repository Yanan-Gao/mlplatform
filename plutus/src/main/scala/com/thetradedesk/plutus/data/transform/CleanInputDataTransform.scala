package com.thetradedesk.plutus.data.transform


import com.thetradedesk.plutus.data.plutusDataPath
import com.thetradedesk.plutus.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions.{col, round, when}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDate

object CleanInputDataTransform {

  val DEFAULT_NUM_PARTITION = 200

  def transform(date: LocalDate, ttdEnv: String, rawInputS3Path: String, rawInputPrefix: String, extremeValueThreshold: Double, svName: Option[String] = None, cleanOutputS3Path: String, cleanOutputPrefix: String)(implicit prometheus: PrometheusClient): Unit = {
    val inputPath = plutusDataPath(rawInputS3Path, ttdEnv, rawInputPrefix, svName, date)

    val cleanData = createCleanDataset(inputPath, extremeValueThreshold)

    val totalData = prometheus.createGauge("clean_data_rows", "count of processed rows")
    val mbwDataCount = prometheus.createGauge("clean_data_rows_mb2w", "Total Data with MB2W")
    val mbwBidsCount = prometheus.createGauge("clean_data_bids_mb2w", "Total Bids with MB2W")
    val mbwImpsCount = prometheus.createGauge("clean_data_impressions_mb2w", "Total Imps with MB2W")
    val mbwValidBidsCount = prometheus.createGauge("clean_data_bids_valid_mb2w", "Total Bids with Valid MB2W")
    val mbwValidImpsCount = prometheus.createGauge("clean_data_impressions_valid_mb2w", "Total Impressions with Valid MB2W")

    totalData.set(cleanData.cache().count)
    mbwDataCount.set(cleanData.filter(col("mb2w").isNotNull).count)
    mbwBidsCount.set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNull).count)
    mbwImpsCount.set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNotNull).count)

    // clean bids t
    mbwValidBidsCount.set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNull).filter(col("mb2w") >= col("b_RealBidPrice")).count)

    mbwValidImpsCount.set(cleanData.filter(col("mb2w").isNotNull && col("RealMediaCost").isNotNull).filter(col("mb2w") <= col("RealMediaCost")).count)

    val outputPath = plutusDataPath(cleanOutputS3Path, ttdEnv, cleanOutputPrefix, svName, date)
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
      .withColumn("AuctionBidPrice",
        when(col("RealMediaCost").isNotNull, col("RealMediaCost"))
          .when((col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD"))), col("FloorPriceInUSD"))
          .otherwise(col("b_RealBidPrice")))
      .filter(col("valid") === true)
      .drop("valid")

    ds.selectAs[CleanInputData]
  }
}
