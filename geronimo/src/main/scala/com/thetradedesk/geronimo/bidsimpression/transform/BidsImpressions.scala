package com.thetradedesk.geronimo.bidsimpression.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidCols, BidsImpressionsSchema, ContextualCategoryRecord, ImpressionsCols}
import com.thetradedesk.geronimo.shared.explicitDatePart
import com.thetradedesk.geronimo.shared.schemas.{AdvertiserRecord, BidFeedbackRecord, BidRequestRecord}
import com.thetradedesk.logging.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.{col, when, xxhash64}
import org.apache.spark.sql.functions._

import scala.util.Random
import java.net.{MalformedURLException, URL}
import java.time.LocalDate

case class BidsImpressionsMetrics (
  ContextualCoverage: Double
)

object BidsImpressions extends Logger {

  val TIME_ROUNDING_PRECISION = 6
  // used for time of day features.  Pi is hardcoded here because the math.pi values differ between java/scala
  // and c# where the time of day features are computed at bid time.
  val TWOPI = 2 * 3.14159265359

  def transform(
                 bids: Dataset[BidRequestRecord],
                 impressions: Dataset[BidFeedbackRecord],
                 advertiser: Dataset[AdvertiserRecord],
                 contextual: Dataset[ContextualCategoryRecord],
                 numberOfBucket: Int,
                 numberOfPopularUrl: Int
               ): (Dataset[BidsImpressionsSchema], BidsImpressionsMetrics) = {

    val bidsDf = joinBidsWithContextual(bids, contextual, numberOfBucket, numberOfPopularUrl)

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

    val metrics = BidsImpressionsMetrics(
      ContextualCoverage = joinDf.filter($"ContextualCategories".isNotNull).count() / joinDf.count().toDouble
    )

    (joinDf, metrics)
  }

  def joinBidsWithContextual(bids: Dataset[BidRequestRecord],
                             contextual: Dataset[ContextualCategoryRecord],
                             numberOfBucket: Int,
                             numberOfPopularUrl: Int): DataFrame = {

    // Get the list of top 200 urls from bid request data
    // These are the two most popular sites (~10% of all bid requests) without a contextual categorization
    // Filtering them out in multiple places to save time in the join
    val popularUrls = bids.select("ReferrerUrl")
      .filter($"ReferrerUrl".isNotNull && !$"ReferrerUrl".isin("https://mail.yahoo.com", "com.hulu.plus.roku"))
      .withColumn("NormalizedUrl", normalizeUrlUDF($"ReferrerUrl"))
      .groupBy($"NormalizedUrl").count.orderBy(desc("count")).limit(numberOfPopularUrl)
      .cache.persist

    // The popular urls deserve special handling because anyone of them will be too big to fit in one partition
    // Create the new JoinKey (url + random bucket) to further partition the data
    val contextualJoinBuckets = 0.until (numberOfBucket).toDF("Bucket")
    val popularContextual = contextual
      .filter(isStandardCategoryUdf($"ContextualCategoryId"))
      .withColumn("ReferrerUrl", when($"InApp", $"UrlHost")
        .otherwise(concat($"UrlSchema", lit("://"),  $"UrlHost", $"UrlPath")))
      .withColumn("NormalizedUrl", normalizeUrlUDF($"ReferrerUrl"))
      .join(broadcast(popularUrls), "NormalizedUrl")
      .groupBy("NormalizedUrl")
      .agg(collect_set($"ContextualCategoryId").as("ContextualCategories"))
      .crossJoin(contextualJoinBuckets)
      .withColumn("ContextualJoinKey", xxhash64(concat($"NormalizedUrl", $"Bucket")))
      .select($"ContextualJoinKey", $"ContextualCategories")
      .repartition($"ContextualJoinKey")
      .cache.persist

    // The non-popular urls will only use the hash of url for joining
    val normalContextual = contextual
      .filter(isStandardCategoryUdf($"ContextualCategoryId"))
      .withColumn("ReferrerUrl", when($"InApp", $"UrlHost")
        .otherwise(concat($"UrlSchema", lit("://"),  $"UrlHost", $"UrlPath")))
      .withColumn("NormalizedUrl", normalizeUrlUDF($"ReferrerUrl"))
      .join(broadcast(popularUrls), Seq("NormalizedUrl"), "leftanti")
      .withColumn("ContextualJoinKey", xxhash64($"NormalizedUrl"))
      .groupBy("ContextualJoinKey")
      .agg(collect_set($"ContextualCategoryId").as("ContextualCategories"))
      .repartition($"ContextualJoinKey")
      .cache.persist

    // Bid request data is separated into three datasets: bids with popular urls, bids with non-popular urls, and bids without urls
    val bidsWithPopularUrl = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*).filter($"ReferrerUrl".isNotNull
      && !$"ReferrerUrl".isin("https://mail.yahoo.com", "com.hulu.plus.roku"))
      .withColumn("NormalizedUrl", normalizeUrlUDF($"ReferrerUrl"))
      .join(broadcast(popularUrls), "NormalizedUrl")

    val bidsWithNormalUrl = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*).filter($"ReferrerUrl".isNotNull
      && !$"ReferrerUrl".isin("https://mail.yahoo.com", "com.hulu.plus.roku"))
      .withColumn("NormalizedUrl", normalizeUrlUDF($"ReferrerUrl"))
      .join(broadcast(popularUrls), Seq("NormalizedUrl"), "leftanti")

    val bidsWithoutUrl = bids.select(BidCols.BIDSCOLUMNS.map(i => col(i)): _*).filter($"ReferrerUrl".isNull
      || $"ReferrerUrl".isin("https://mail.yahoo.com", "com.hulu.plus.roku"))

    // Same as handling popular url contextuals, here we create a new JoinKey (url + random bucket) on bids data for the join
    val bidsWithPopularUrlDf = bidsWithPopularUrl
      .withColumn("Bucket", lit(Random.nextInt(numberOfBucket)))
      .withColumn("ContextualJoinKey", xxhash64(concat($"NormalizedUrl", $"Bucket")))
      .repartition($"ContextualJoinKey")
      .join(broadcast(popularContextual), Seq("ContextualJoinKey"), "leftouter")
      .drop("NormalizedUrl", "Bucket", "ContextualJoinKey", "Count")

    // Same as handling non-popular url contextuals, here we just use hash of url on bids data for the join
    val bidsWithNormalUrlDf = bidsWithNormalUrl
      .withColumn("ContextualJoinKey", xxhash64($"NormalizedUrl"))
      .repartition($"ContextualJoinKey")
      .join(normalContextual, Seq("ContextualJoinKey"), "leftouter")
      .drop("NormalizedUrl", "ContextualJoinKey")

    val bidsWithoutUrlDf = bidsWithoutUrl
      .withColumn("ContextualCategories", lit(null: String))

    bidsWithPopularUrlDf.union(bidsWithNormalUrlDf).union(bidsWithoutUrlDf)
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .repartition(col("BidRequestIdHash"))
      .withColumn("UIID", coalesce($"DeviceAdvertisingId", $"TDID", $"UnifiedId2"))
  }

  def writeOutput(rawData: Dataset[BidsImpressionsSchema], outputPath: String, ttdEnv: String, outputPrefix: String, date: LocalDate, inputHours: Seq[Int], writePartitions: Int): Unit = {
     // note the date part is year=yyyy/month=m/day=d/
      rawData
        .coalesce(writePartitions)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}/hourPart=${inputHours.min}/")
    }

  def normalizeUrl(referrerUrl: String): String = {
    try {
      val myURL = new URL(referrerUrl)
      val host : String = if(myURL.getHost() == null) null else myURL.getHost().stripSuffix("/").stripPrefix("/")
      val path : String  = if(myURL.getPath() == null) null else myURL.getPath().stripSuffix("/")
      (host + path).toLowerCase()
    }
    catch {
      case x: MalformedURLException => if (referrerUrl == null) null else referrerUrl.toLowerCase()  // Could be an app id or whatever malformatted
    }
  }
  val normalizeUrlUDF = udf(normalizeUrl _)

  def isStandardCategory(categoryId: Long) : Boolean =
    if ((categoryId & 0x1000000) != 0) true else false

  def isStandardCategoryUdf = udf(isStandardCategory _)
}
