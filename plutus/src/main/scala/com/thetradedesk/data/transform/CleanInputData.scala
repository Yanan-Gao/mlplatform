package com.thetradedesk.data.transform

import com.thetradedesk.data.datePart
import com.thetradedesk.data.schema.CleanInputData
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import org.apache.spark.sql.functions.{col, round, when}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDate

object CleanInputData {

  val DEFAULT_SV_NAME = "google"

  def cleanDataS3Path(s3Path: String, prefix: String, date: LocalDate, svName: Option[String] = None) = {
    s"$s3Path/$prefix/${svName.getOrElse(DEFAULT_SV_NAME)}/${datePart(date)}"
  }


  def loadRawInputDataframe(s3Path: String, prefix: String, date: LocalDate, svName: Option[String] = None)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(cleanDataS3Path(s3Path, prefix, date, svName))
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))
  }

  def createCleanDataset(df: DataFrame)(implicit spark: SparkSession): Dataset[CleanInputData] = {
    import spark.implicits._
    df.selectAs[CleanInputData]
  }

  def createCleanDataframe(rawInputS3Path: String, rawInputPrefix: String, date: LocalDate, extremeValueThreshold: Double, svName: Option[String] = None)(implicit spark: SparkSession) = {

    val df = loadRawInputDataframe(rawInputS3Path, rawInputPrefix, date, svName)
    //TODO: counters on size etc here

    df
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
      .filter(col("valid") === true)
      .drop("valid")

    //TODO: counters here?
  }

  def createTabularTFRecordDataframe(cleanData: Dataset[CleanInputData])(implicit spark: SparkSession) = {

  }

}
