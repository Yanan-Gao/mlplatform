package com.thetradedesk.data

import java.time.LocalDate

import com.thetradedesk.data.schema.{BidRequestDataset, BidRequestRecordV4, EmpiricalDiscrepancy}
import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import io.prometheus.client.Gauge
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{broadcast, coalesce, col, concat_ws, cos, dayofweek, hour, lit, minute, round, sin, when}

class Bids {

  /**TODO: define output schema and clean up input schema (now just full br from eldorado)
   *
   * @param date
   * @param lookBack
   * @param svName
   * @param svbDf
   * @param pdaDf
   * @param dealDf
   * @param empDisDf
   * @param bidsGauge
   * @param spark
   * @return DataFrame
   */

  def getBidsData(date: LocalDate, lookBack: Int, svName: String, svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame, empDisDf: Dataset[EmpiricalDiscrepancy], bidsGauge: Gauge)(implicit spark: SparkSession) = {

    import spark.implicits._

    val bidsDf = getParquetData[BidRequestRecordV4](date, lookBack, BidRequestDataset.BIDSS3).alias("bids")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .filter(col("SupplyVendor") === svName)
      .filter(col("AuctionType") === 1)

      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pdaDf).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svbDf).alias("svb"), Seq("SupplyVendor"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .withColumn("bid_adjuster",
        coalesce('FirstPriceAdjustment,
          lit(1)/coalesce(
            'EmpiricalDiscrepancy,
            'pdaDiscrpancyAdjustment,
            'svbDiscrpancyAdjustment,
            lit(1)
          )
        )
      )
      .withColumn("b_RealBidPriceInUSD", 'AdjustedBidCPMInUSD * 'bid_adjuster)
      .withColumn("b_RealBidPrice", round('b_RealBidPriceInUSD, 3))
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
        (sin(lit(2 * 3.14159265359) * hour(col("LogEntryTime")) / 24)).alias("sin_hour_day"),
        (cos(lit(2 * 3.14159265359) * hour(col("LogEntryTime")) / 24)).alias("cos_hour_day"),
        // hour in the week
        (sin(lit(2 * 3.14159265359) * (hour(col("LogEntryTime")) + ( dayofweek(col("LogEntryTime")) * 24 )) / (7*24) )).alias("sin_hour_week"),
        (cos(lit(2 * 3.14159265359) * (hour(col("LogEntryTime")) + ( dayofweek(col("LogEntryTime")) * 24 )) / (7*24) )).alias("cos_hour_week"),
        // minute in the hour
        (sin(lit(2 * 3.14159265359) * minute(col("LogEntryTime")) / 60)).alias("sin_minute_hour"),
        (cos(lit(2 * 3.14159265359) * minute(col("LogEntryTime")) / 60)).alias("cos_minute_hour"),
        // minute in the week
        (sin(lit(2 * 3.14159265359) * (minute(col("LogEntryTime")) + ( hour(col("LogEntryTime")) * 60 ) )  / (24*60) )).alias("sin_minute_day"),
        (cos(lit(2 * 3.14159265359) * (minute(col("LogEntryTime")) + ( hour(col("LogEntryTime")) * 60 ) )  / (24*60) )).alias("cos_minute_day"),


        // Seller/Publisher Features
        // BID ??
        col("AdsTxtSellerType.value").alias("AdsTxtSellerType"),
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/TTD.Domain.Bidding.Public/RTB/Bidding/PublisherType.cs
        col("PublisherType.value").alias("PublisherType"),

        // Seems to be just identifying unknown carrier
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Bidding/Bidder/TTD.Domain.Bidding.Bidder/Adapters/GoogleAdapter.cs#L899
        // BID: https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/Bidding/InternetConnectionType.cs
        col("InternetConnectionType.value").alias("InternetConnectionType"),
        col("Carrier"),


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
    bidsGauge.set(bidsDf.count())
    bidsDf
  }

}
