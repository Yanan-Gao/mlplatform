package com.thetradedesk.geronimo.bidsimpression.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidCols, BidsImpressionsSchema, ImpressionsCols}
import com.thetradedesk.geronimo.shared.explicitDatePart
import com.thetradedesk.geronimo.shared.schemas.{AdvertiserRecord, BidFeedbackRecord, BidRequestRecord}
import com.thetradedesk.logging.Logger
import org.apache.spark.sql.{Dataset, SaveMode}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.{col, when, xxhash64}
import org.apache.spark.sql.functions._

import java.time.LocalDate

object BidsImpressions extends Logger {

  val TIME_ROUNDING_PRECISION = 6
  // used for time of day features.  Pi is hardcoded here because the math.pi values differ between java/scala
  // and c# where the time of day features are computed at bid time.
  val TWOPI = 2 * 3.14159265359

  def transform(
                 bids: Dataset[BidRequestRecord],
                 impressions: Dataset[BidFeedbackRecord],
                 advertiser: Dataset[AdvertiserRecord]
               ): Dataset[BidsImpressionsSchema] = {

    val bidsDf = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .withColumn("UIID", coalesce($"DeviceAdvertisingId", $"TDID", $"UnifiedId2"))
      .repartition(col("BidRequestIdHash"))

    val impressionsDf = impressions.select(ImpressionsCols.IMPRESSIONCOLUMNS.map(i => col(i)): _*)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .drop("BidRequestId")
      .repartition(col("BidRequestIdHash"))

    val joinDf = bidsDf.alias("bids")
      .join(impressionsDf.alias("i"), Seq("BidRequestIdHash"), "leftouter")
      .join(advertiser.select($"AdvertiserId", $"IndustryCategoryId" as "AdvertiserIndustryCategoryId"),Seq("AdvertiserId"), "left")
      .withColumn("IsImp", when(col("MediaCostCPMInUSD").isNotNull, true).otherwise(false))
      .withColumn("ImpressionsFirstPriceAdjustment", col("i.FirstPriceAdjustment"))
      .withColumn("BidsFirstPriceAdjustment", col("bids.FirstPriceAdjustment"))
      // https://ianlondon.github.io/blog/encoding-cyclical-features-24hour-time/ (also from Victor)
      // hour in the day
      .withColumn("sin_hour_day", round(sin(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
      .withColumn("cos_hour_day", round(cos(lit(TWOPI) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION))
      // hour in the week
      .withColumn("sin_hour_week", round(sin(lit(TWOPI) * (hour(col("LogEntryTime")) + (dayofweek(col("LogEntryTime")) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
      .withColumn("cos_hour_week" , round(cos(lit(TWOPI) * (hour(col("LogEntryTime")) + (dayofweek(col("LogEntryTime")) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION))
      // minute in the hour
      .withColumn("sin_minute_hour", round(sin(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
      .withColumn("cos_minute_hour" , round(cos(lit(TWOPI) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION))
      // minute in the week
      .withColumn("sin_minute_day", round(sin(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))
      .withColumn("cos_minute_day", round(cos(lit(TWOPI) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION))

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
