package com.thetradedesk.plutus.data.transform

import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.transform.RawDataTransform.writeOutput
import com.thetradedesk.plutus.data.{cleansedDataPaths, explicitDatePart, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import io.prometheus.client.Gauge
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object RawDataTransform {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2
  val TIME_ROUNDING_PRECISION = 6

  def transform(date: LocalDate, outputPath: String, ttdEnv: String, outputPrefix: String, svName: String)(implicit prometheus: PrometheusClient): Unit = {

    val googleLostBidData = googleMinimumBidToWinData(date)

    val (svbDf, pdaDf, dealDf) = discrepancyData(date)

    val impressionsGauge: Gauge = prometheus.createGauge("raw_impressions_count", "count of raw impressions")
    val (imps, empiricalDiscrepancyDf) = adjustedImpressions(date, svName, svbDf, pdaDf, dealDf.toDF)
    impressionsGauge.set(imps.count())


    val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")
    val bids = bidsData(date, svName, svbDf, pdaDf, dealDf, empiricalDiscrepancyDf)
    bidsGauge.set(bids.count())


    val rawData = bids
      .join(imps, Seq("BidRequestId"), "left")
      .join(googleLostBidData, Seq("BidRequestId"), "left")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))

    writeOutput(rawData, outputPath, ttdEnv, outputPrefix, svName, date)
  }

  def writeOutput(rawData: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, svName: String, date: LocalDate): Unit = {
    // note the date part is year=yyyy/month=m/day=d/
    rawData
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$outputPath/$ttdEnv/$outputPrefix/$svName/${explicitDatePart(date)}")
  }

  def googleMinimumBidToWinData(date: LocalDate): Dataset[GoogleMinimumBidToWinData] = {
    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(GoogleMinimumBidToWinDataset.SCHEMA)
      .load(cleansedDataPaths(GoogleMinimumBidToWinDataset.S3PATH, date): _*)
      .filter(col("sv") === "google")
      // is only set for won bids or mode 79
      .filter((col("svLossReason") === "1") || (col("svLossReason") === "79"))
      .filter(col("winCPM") =!= 0.0 || col("mb2w") =!= 0.0)
      .select(
        col("BidRequestId").cast("String"),
        col("svLossReason").cast("Integer"),
        col("ttdLossReason").cast("Integer"),
        col("winCPM").cast("Double"),
        col("mb2w").cast("Double")
      ).as[GoogleMinimumBidToWinData]
  }

  def discrepancyData(date: LocalDate): (DataFrame, DataFrame, DataFrame) = {
    val svbDf = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
      .withColumn("SupplyVendor", col("RequestName"))
      .withColumn("svbDiscrpancyAdjustment", col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("RequestName")


    val pdaDf = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
      .withColumn("SupplyVendor", col("SupplyVendorName"))
      .withColumn("pdaDiscrpancyAdjustment", col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("SupplyVendorName")

    val dealDf = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
      .join(svbDf, "SupplyVendorId")
      .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))

    (svbDf, pdaDf, dealDf)
  }

  def adjustedImpressions(date: LocalDate, svName: String, svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame): (Dataset[AdjImpressions], Dataset[EmpiricalDiscrepancy]) = {
    val impressions = loadParquetData[Impressions](s3path = BidFeedbackDataset.BFS3, date = date)
      .filter(col("SupplyVendor") === svName)
      .filter(col("AuctionType") === "FirstPrice")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))

    val empiricalDiscrepancyDf = impressions.alias("bf")
      .select(
        col("PartnerId"),
        col("SupplyVendor"),
        col("DealId"),
        col("AdFormat"),
        coalesce(col("DiscrepancyAdjustmentMultiplier"), lit(1)).alias("DiscrepancyAdjustmentMultiplier")
      )
      .groupBy("PartnerId", "SupplyVendor", "DealId", "AdFormat")
      .agg(round(avg("DiscrepancyAdjustmentMultiplier"), EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION).as("EmpiricalDiscrepancy"))


    val impsDf = impressions.alias("bf")
      .join(empiricalDiscrepancyDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pdaDf).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svbDf).alias("svb"), Seq("SupplyVendor"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .withColumn("imp_adjuster",
        coalesce(
          'FirstPriceAdjustment / 'DiscrepancyAdjustmentMultiplier,
          lit(1) / coalesce(
            'DiscrepancyAdjustmentMultiplier,
            'EmpiricalDiscrepancy,
            'pdaDiscrpancyAdjustment,
            'svbDiscrpancyAdjustment,
            lit(1)
          )
        )
      )
      .withColumn("RealMediaCostInUSD", 'MediaCostCPMInUSD / 'DiscrepancyAdjustmentMultiplier)
      .withColumn("RealMediaCost", round('RealMediaCostInUSD, ROUNDING_PRECISION))
      .withColumn("i_RealBidPriceInUSD", 'SubmittedBidAmountInUSD * 1000 * 'imp_adjuster)
      .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, ROUNDING_PRECISION))

      // only select variable priced deals
      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .select(
        col("BidRequestId"),
        col("DealId").alias("i_DealId"),

        col("MediaCostCPMInUSD").cast(DoubleType).alias("MediaCostCPMInUSD"),
        col("RealMediaCostInUSD").cast(DoubleType).alias("RealMediaCostInUSD"),
        col("RealMediaCost").cast(DoubleType).alias("RealMediaCost"),
        col("DiscrepancyAdjustmentMultiplier").cast(DoubleType).alias("DiscrepancyAdjustmentMultiplier"),

        col("i_RealBidPrice"),
        (col("SubmittedBidAmountInUSD") * 1000).cast(DoubleType).alias("i_OrigninalBidPrice"),
        col("FirstPriceAdjustment").cast(DoubleType).alias("i_FirstPriceAdjustment"),
        col("imp_adjuster")
      )


    (impsDf.selectAs[AdjImpressions], empiricalDiscrepancyDf.selectAs[EmpiricalDiscrepancy])

  }


  def bidsData(date: LocalDate, svName: String, svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame, empDisDf: Dataset[EmpiricalDiscrepancy]): DataFrame = {
    val bidsDf = loadParquetData[BidRequestRecordV4](BidRequestDataset.BIDSS3, date).alias("bids")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .filter(col("SupplyVendor") === svName)
      .filter(col("AuctionType") === 1) // Note the difference with Impression Data

      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pdaDf).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svbDf).alias("svb"), Seq("SupplyVendor"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .withColumn("bid_adjuster",
        coalesce('FirstPriceAdjustment,
          lit(1) / coalesce(
            'EmpiricalDiscrepancy,
            'pdaDiscrpancyAdjustment,
            'svbDiscrpancyAdjustment,
            lit(1)
          )
        )
      )
      .withColumn("b_RealBidPriceInUSD", 'AdjustedBidCPMInUSD * 'bid_adjuster)
      .withColumn("b_RealBidPrice", round('b_RealBidPriceInUSD, ROUNDING_PRECISION))
      .withColumn("PredictiveClearingRandomControl", when(col("PredictiveClearingRandomControl"), 1).otherwise(0))

      // only select variable priced deals
      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .select(
        col("BidRequestId"),
        col("DealId"),

        col("AdjustedBidCPMInUSD").cast("double").alias("AdjustedBidCPMInUSD"),
        col("FirstPriceAdjustment").cast("Double").alias("b_FirstPriceAdjustment"),
        col("FloorPriceInUSD").cast("double").alias("FloorPriceInUSD"),


        // calculated values
        col("b_RealBidPriceInUSD").cast("double").alias("b_RealBidPriceInUSD"),
        col("b_RealBidPrice").cast("double").alias("b_RealBidPrice"),
        col("bid_adjuster").cast("double").alias("bid_adjuster"),


        // Identifiers
        col("PartnerId"),
        col("AdvertiserId"),
        col("CampaignId"),
        col("AdGroupId"),


        // Contextual
        col("SupplyVendor"),
        col("SupplyVendorPublisherId"),
        col("SupplyVendorSiteId"),
        col("Site"),
        col("ImpressionPlacementId"),
        // https://atlassian.thetradedesk.com/confluence/display/TSDKB/Category+Tile+-+Site+List
        // availabe at bid time (maybe)
        // https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/TTD.Domain.Bidding.Public/RTB/Bidding/Bid.cs#L51
        col("MatchedCategory"),
        // BID: Maybe (??) https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/Bidder/TTD.Domain.Bidding.Bidder/Adapters/GoogleAdapter.cs#L1350
        col("MatchedFoldPosition"),
        col("RenderingContext.value").alias("RenderingContext"),
        col("AdFormat"),



        // TODO: not sure if this is available depending on where PC is actually called
        col("VolumeControlPriority"),


        // Temporal Features
        col("LogEntryTime"),
        // https://ianlondon.github.io/blog/encoding-cyclical-features-24hour-time/ (also from Victor)
        // hour in the day
        round(sin(lit(2 * 3.14159265359) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION).alias("sin_hour_day"),
        round(cos(lit(2 * 3.14159265359) * hour(col("LogEntryTime")) / 24), TIME_ROUNDING_PRECISION).alias("cos_hour_day"),
        // hour in the week
        round(sin(lit(2 * 3.14159265359) * (hour(col("LogEntryTime")) + (dayofweek(col("LogEntryTime")) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION).alias("sin_hour_week"),
        round(cos(lit(2 * 3.14159265359) * (hour(col("LogEntryTime")) + (dayofweek(col("LogEntryTime")) * 24)) / (7 * 24)), TIME_ROUNDING_PRECISION).alias("cos_hour_week"),
        // minute in the hour
        round(sin(lit(2 * 3.14159265359) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION).alias("sin_minute_hour"),
        round(cos(lit(2 * 3.14159265359) * minute(col("LogEntryTime")) / 60), TIME_ROUNDING_PRECISION).alias("cos_minute_hour"),
        // minute in the week
        round(sin(lit(2 * 3.14159265359) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION).alias("sin_minute_day"),
        round(cos(lit(2 * 3.14159265359) * (minute(col("LogEntryTime")) + (hour(col("LogEntryTime")) * 60)) / (24 * 60)), TIME_ROUNDING_PRECISION).alias("cos_minute_day"),


        // Seller/Publisher Features
        // BID ??
        col("AdsTxtSellerType.value").alias("AdsTxtSellerType"),
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/TTD.Domain.Bidding.Public/RTB/Bidding/PublisherType.cs
        col("PublisherType.value").alias("PublisherType"),

        // Seems to be just identifying unknown carrier
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/Bidder/TTD.Domain.Bidding.Bidder/Adapters/GoogleAdapter.cs#L899
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/Bidding/InternetConnectionType.cs
        // TODO add to bidrequest dataset
        //  col("InternetConnectionType.value").alias("InternetConnectionType"),
        //  col("Carrier"),


        // Geo Features
        col("Country"),
        col("Region"),
        col("Metro"),
        col("City"),
        col("Zip"),

        // Device Features
        col("DeviceType.value").alias("DeviceType"),
        col("DeviceMake"),
        col("DeviceModel"),
        col("OperatingSystemFamily.value").alias("OperatingSystemFamily"),
        col("Browser.value").alias("Browser"),

        // User features
        // Not sure if this is UTC but should not hurt to keep it
        col("UserHourOfWeek"),
        // BID: maybe https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/TTD.Domain.Bidding.Public/RTB/Bidding/Bid.cs#L140
        col("RequestLanguages"),
        // This could be Geo features but they are the lat/long of the user so may be better placed in user features
        col("Latitude"),
        col("Longitude"),

        // PC Features - useful for eval but will not be model input
        col("PredictiveClearingMode.value").alias("PredictiveClearingMode"),
        col("PredictiveClearingRandomControl")
      )
    bidsDf
  }
}
