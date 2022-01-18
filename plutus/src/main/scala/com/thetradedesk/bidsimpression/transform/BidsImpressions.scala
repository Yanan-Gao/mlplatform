package com.thetradedesk.bidsimpression.transform

import com.thetradedesk.bidsimpression.schema.{BidCols, BidsImpressionsSchema, ImpressionsCols}
import com.thetradedesk.plutus.data.{cacheToHDFS, explicitDatePart, loadParquetData}
import com.thetradedesk.plutus.data.schema.{BidFeedbackDataset, BidRequestDataset, BidRequestRecord, Impressions}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.{col, when, xxhash64}

import java.time.LocalDate

object BidsImpressions {

  def transform(date: LocalDate, outputPath: String, ttdEnv: String, outputPrefix: String, bids: Dataset[BidRequestRecord], impressions: Dataset[Impressions], isTest: Boolean = false) = {

    val bidsDf = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .drop("BidRequestId")
      .repartition(col("BidRequestIdHash"))

    val impressionsDf = impressions.select(ImpressionsCols.IMPRESSIONCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .repartition(col("BidRequestIdHash"))


    val joinDf = bidsDf.alias("bids")
      .join(impressionsDf.alias("i"), Seq("BidRequestIdHash"), "leftouter")
      .withColumn("IsImp", when(col("MediaCostCPMInUSD").isNotNull, 1).otherwise(0))
      .withColumn("ImpressionsFirstPriceAdjustment", col("i.FirstPriceAdjustment"))
      .withColumn("BidsFirstPriceAdjustment", col("bids.FirstPriceAdjustment"))
      .selectAs[BidsImpressionsSchema]
      .cache

    // val writeDs = cacheToHDFS[BidsImpressionsSchema](joinDf)

    println(joinDf.count())

    writeOutput(joinDf, outputPath, ttdEnv, outputPrefix, date, isTest)
  }

  def writeOutput(rawData: Dataset[BidsImpressionsSchema], outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate, isTest: Boolean = false): Unit = {
    if (isTest) {
      rawData.collect()
    }
    else {
      // note the date part is year=yyyy/month=m/day=d/
      rawData
        .write
        .mode(SaveMode.Append)
        .parquet(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}/")
    }
  }
}
