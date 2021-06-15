package com.thetradedesk.data.transform

import com.thetradedesk.data.{explicitDatePart, plutusDataPath}
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import org.apache.spark.sql.functions.{col, round, when}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDate
import io.prometheus.client.Gauge

object CleanInputDataTransform {

  def loadRawInputDataframe(s3Path: String, ttdEnv: String, prefix: String, date: LocalDate, svName: Option[String] = None): DataFrame = {
    spark.read.parquet(plutusDataPath(s3Path, ttdEnv, prefix, svName, date))
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))
  }



  def createCleanDataset(rawInputS3Path: String, ttdEnv: String, rawInputPrefix: String, date: LocalDate, extremeValueThreshold: Double, svName: Option[String] = None, cleanDataOutputCount: Gauge): Dataset[CleanInputData] = {

    val df = loadRawInputDataframe(rawInputS3Path, ttdEnv, rawInputPrefix, date, svName)
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
          .when( (col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD"))), col("FloorPriceInUSD"))
          .otherwise(col("b_RealBidPrice")))
          .filter(col("valid") === true)
      .drop("valid")

    cleanDataOutputCount.set(ds.count())

    ds.selectAs[CleanInputData]

  }


}
