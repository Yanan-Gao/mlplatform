package com.thetradedesk.plutus.data.transform


import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.schemas.EmpiricalDiscrepancy
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.explicitDatePart
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.parquet.format.IntType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object RawDataTransform extends Logger {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2

  def transform(date: LocalDate, svNames: Seq[String], bidsImpressions: Dataset[BidsImpressionsSchema], rawLostBidData: Dataset[RawLostBidData], discrepancy: (Dataset[Svb], Dataset[Pda], Dataset[Deals]), partitions: Int)(implicit prometheus: PrometheusClient): DataFrame = {

    val mb2wData = minimumBidToWinData(rawLostBidData, svNames).repartition(partitions)

    log.info("lost bid data " + mb2wData.cache.count())

    val dealDf = dealData(discrepancy._1, discrepancy._3)

    val empiricalDiscrepancyDf = empiricalImpressions(bidsImpressions)

    discrepancy._1.cache.count()
    discrepancy._2.cache.count()
    dealDf.cache().count()
    empiricalDiscrepancyDf.cache.count()

    val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")
    val bids = allData(date, svNames, bidsImpressions, discrepancy._1, discrepancy._2, dealDf, empiricalDiscrepancyDf, partitions)
      .repartition(partitions)
      .cache()

    bidsGauge.set(bids.count())

    val rawData = bids
      .join(mb2wData, Seq("BidRequestId"), "left")

    rawData
  }

  def writeOutput(rawData: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, svName: String, date: LocalDate): Unit = {
      // note the date part is year=yyyy/month=m/day=d/
      rawData
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"$outputPath/$ttdEnv/$outputPrefix/$svName/${explicitDatePart(date)}")
  }

  def minimumBidToWinData(rawLostBidData: Dataset[RawLostBidData], svNames: Seq[String]): Dataset[MinimumBidToWinData] = {
    rawLostBidData
      .filter(col("SupplyVendor").isin(svNames: _*))
      // https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/LossReason.cs
      .filter((col("LossReason") === "-1") || (col("LossReason") === "102"))
      .filter(col("WinCPM") =!= 0.0 || col("mb2w") =!= 0.0)
      .select(
        col("BidRequestId").cast("String"),
        col("SupplyVendorLossReason").cast("Integer"),
        col("LossReason").cast("Integer"),
        col("WinCPM").cast("Double"),
        col("mb2w").cast("Double")
      ).as[MinimumBidToWinData]
  }

  def dealData(svb: Dataset[Svb], deals: Dataset[Deals]): DataFrame = {

    val dealDf = deals
      .join(svb.alias("svb"), "SupplyVendorId")
      .withColumn("SupplyVendor", col("svb.RequestName"))
      .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))

    dealDf
  }

  def empiricalImpressions(bidsImpressions: Dataset[BidsImpressionsSchema]): Dataset[EmpiricalDiscrepancy] = {
    val impressions = bidsImpressions
      .filter(col("AuctionType") === "FirstPrice" && col("IsImp"))
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

    empiricalDiscrepancyDf.selectAs[EmpiricalDiscrepancy]

  }


  def allData(date: LocalDate, svNames: Seq[String], bidsImpressions: Dataset[BidsImpressionsSchema], svb: Dataset[Svb], pda: Dataset[Pda], dealDf: DataFrame, empDisDf: Dataset[EmpiricalDiscrepancy], partitions: Int): DataFrame = {
    val bidsDf = bidsImpressions.alias("bids")
      .filter(col("AuctionType") === 1 && $"SupplyVendor".isin(svNames: _*)) // Note the difference with Impression Data
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))

      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pda.withColumn("SupplyVendor", col("SupplyVendorName"))).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svb).alias("svb"), col("SupplyVendor") === col("RequestName"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .drop("svb.RequestName")
      .drop("pda.SupplyVendorName")
      .repartition(partitions)

      // determining how much the bid was adjusted by to back out real bid price
      .withColumn("bid_adjuster",
        coalesce('BidsFirstPriceAdjustment,
          lit(1) / coalesce(
            col("EmpiricalDiscrepancy"),
            col("pda.DiscrepancyAdjustment"),
            col("svb.DiscrepancyAdjustment"),
            lit(1)
          )
        )
      )

      // same as above, figuring out all adjustments for in bidfeedback
      .withColumn("imp_adjuster",
        coalesce(
          'ImpressionsFirstPriceAdjustment / 'DiscrepancyAdjustmentMultiplier,
          lit(1) / coalesce(
            col("DiscrepancyAdjustmentMultiplier"),
            col("EmpiricalDiscrepancy"),
            col("pda.DiscrepancyAdjustment"),
            col("svb.DiscrepancyAdjustment"),
            lit(1)
          )
        )
      )

      .withColumn("RealMediaCostInUSD", 'MediaCostCPMInUSD / 'DiscrepancyAdjustmentMultiplier)
      .withColumn("RealMediaCost", round('RealMediaCostInUSD, ROUNDING_PRECISION))
      .withColumn("i_RealBidPriceInUSD", 'SubmittedBidAmountInUSD * 1000 * 'imp_adjuster)
      .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, ROUNDING_PRECISION))

      .withColumn("b_RealBidPriceInUSD", 'AdjustedBidCPMInUSD * 'bid_adjuster)
      .withColumn("b_RealBidPrice", round('b_RealBidPriceInUSD, ROUNDING_PRECISION))
      // .withColumn("PredictiveClearingRandomControl", when(col("PredictiveClearingRandomControl"), 1).otherwise(0))

      // only select variable priced deals
      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .select(

        col("BidRequestId"),
        col("DealId"),

        // adjusted impression cols

        col("MediaCostCPMInUSD").cast(DoubleType).alias("MediaCostCPMInUSD"),
        col("RealMediaCostInUSD").cast(DoubleType).alias("RealMediaCostInUSD"),
        col("RealMediaCost").cast(DoubleType).alias("RealMediaCost"),
        col("DiscrepancyAdjustmentMultiplier").cast(DoubleType).alias("DiscrepancyAdjustmentMultiplier"),

        col("i_RealBidPrice"),
        (col("SubmittedBidAmountInUSD") * 1000).cast(DoubleType).alias("ImpressionsOriginalBidPrice"),
        col("ImpressionsFirstPriceAdjustment"),
        col("imp_adjuster"),

        col("AdjustedBidCPMInUSD").cast("double").alias("AdjustedBidCPMInUSD"),
        col("BidsFirstPriceAdjustment"),
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
        col("MatchedCategoryList"),
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
        col("sin_hour_day"),
        col("cos_hour_day"),
        // hour in the week
        col("sin_hour_week"),
        col("cos_hour_week"),
        // minute in the hour
        col("sin_minute_hour"),
        col("cos_minute_hour"),
        // minute in the week
        col("sin_minute_day"),
        col("cos_minute_day"),


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
        col("PredictiveClearingRandomControl"),
        col("IsImp").cast(IntegerType)
      )
    bidsDf

  }
}
