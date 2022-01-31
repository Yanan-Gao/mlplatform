package com.thetradedesk.bidsimpression.transform

import com.thetradedesk.bidsimpression.schema.{BidCols, BidsImpressionsSchema, ImpressionsCols}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.{cacheToHDFS, explicitDatePart, loadParquetData}
import com.thetradedesk.plutus.data.schema.{BidFeedbackDataset, BidRequestDataset, BidRequestRecord, Impressions}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.execution.streaming.continuous.SetWriterPartitions
import org.apache.spark.sql.functions.{col, when, xxhash64}

import java.time.LocalDate

object BidsImpressions extends Logger {

  def transform(date: LocalDate, outputPath: String, ttdEnv: String, outputPrefix: String, bids: Dataset[BidRequestRecord], impressions: Dataset[Impressions], inputHours: Seq[Int]): Dataset[BidsImpressionsSchema] = {

    val bidsDf = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .drop("BidRequestId")
      .repartition(col("BidRequestIdHash"))

    val impressionsDf = impressions.select(ImpressionsCols.IMPRESSIONCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .repartition(col("BidRequestIdHash"))


    val joinDf = bidsDf.alias("bids")
      .join(impressionsDf.alias("i"), Seq("BidRequestIdHash"), "leftouter")
      .withColumn("IsImp", when(col("MediaCostCPMInUSD").isNotNull, true).otherwise(false))
      .withColumn("ImpressionsFirstPriceAdjustment", col("i.FirstPriceAdjustment"))
      .withColumn("BidsFirstPriceAdjustment", col("bids.FirstPriceAdjustment"))
      .selectAs[BidsImpressionsSchema]
      .cache

    log.info("join count" + joinDf.count())

    joinDf
  }

  def writeOutput(rawData: Dataset[BidsImpressionsSchema], outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate, inputHours: Seq[Int], writePartitions: Int): Unit = {
     // note the date part is year=yyyy/month=m/day=d/
      rawData
        .coalesce(writePartitions)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}/hourPart=${inputHours.min}/")
    }
}
