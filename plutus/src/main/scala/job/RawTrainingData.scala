package job

import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import java.time.LocalDate

import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object TrainingData extends Logger {

  val date = config.getDate("date" , LocalDate.now())
  val lookBack = config.getInt("daysOfDat" , 1)
  val svName = config.getString("svName", "google")
  val outputPath = config.getString("outputPath" , "s3://thetradedesk-mlplatform-us-east-1/users/nick.noone/pc/trainingdata")

  val prometheus = new PrometheusClient("Plutus", "TrainingDataEtl")
  val jobDurationTimer = prometheus.createGauge("training_data_raw_etl_runtime", "Time to process 1 day of bids, imppressions, lost bid data").startTimer()
  val impressionsGauge = prometheus.createGauge("raw_impressions_count" , "count of raw impressions")
  val bidsGauge = prometheus.createGauge("raw_bids_count", "count of raw bids")

  def main(args: Array[String]): Unit = {

    val lostBids = getLostBids
    val (svbDf, pdaDf, dealDf) = getDiscrepancyData

    val (imps, empDisDf) = getAdjustedImpressions(svbDf, pdaDf, dealDf)

    val bids = getBidsData(pdaDf, pdaDf, dealDf, empDisDf)

    val bidsImpsDf = bids.join(imps, Seq("BidRequestId"), "left")

    val finalDf = bidsImpsDf.join(lostBids, Seq("BidRequestId"), "left")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))

    // should i break this out by sv?
    finalDf.write.mode(SaveMode.Overwrite).parquet(s"$outputPath/$svName/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}/")

    // clean up
    jobDurationTimer.setDuration()
    prometheus.pushMetrics()
    spark.close()
  }

  // tentative output schema case class(BidRequestId: String, svLossReason: Int, ttdLossReason: Int, winCPM: Double, mb2w: Double)
  def getLostBids() = {

    // would nice to be get to a place where we dont need to define the custom schema
    val customSchema = new StructType()
      .add("LogEntryTime", StringType, true) //0
      .add("BidRequestId", StringType, true) // 1
      .add("creativeId", StringType, true) // 2
      .add("adgroupId", StringType, true) // 3
      .add("throwAway_4", StringType, true) // 4
      .add("throwAway_5", StringType, true) // 5
      .add("TtdPartnerId", StringType, true) // 6
      .add("throwAway_7", StringType, true) // 7
      .add("throwAway_8", StringType, true) // 8
      .add("svLossReason", IntegerType, true) //9
      .add("ttdLossReason", IntegerType, true) // 10
      .add("winCPM", DoubleType, true) // 11
      .add("sv", StringType, true) // 12
      .add("bidRequestTime", StringType, true) // 13
      .add("mb2w", DoubleType, true) // 14

    val lostBidBase = "s3://thetradedesk-useast-logs-2/lostbidrequest/cleansed/"
    val lostBidPaths = ArrayBuffer.empty[String]

    // Q: i think this should be 1 if we are running for yesterday yesterdays data?
    (1 to lookBack).foreach { i =>
      lostBidPaths += lostBidBase + f"${date.minusDays(i).getYear}/${date.minusDays(i).getMonthValue}%02d/${date.minusDays(i).getDayOfMonth}%02d/*/*/*.gz"
    }

    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(customSchema)
      .load(lostBidPaths: _*)
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
      )
  }

    def getParquetData(date: LocalDate, lookBack: Int, path: String, cols: Seq[String] = Seq.empty[String]) = {
      var df = spark.emptyDataFrame
      val paths = ArrayBuffer.empty[String]

      (1 to lookBack).foreach{ i =>
        paths += path + f"date=${date.minusDays(i).getYear}${date.minusDays(i).getMonthValue}%02d${date.minusDays(i).getDayOfMonth}%02d/"
      }

      if (cols.isEmpty) {
        df = spark.read.parquet(paths: _*)
      }
      else {
        df = spark.read.parquet(paths: _*).select(cols.map(i => col(i)): _*)
      }
      df
    }

    def getDiscrepancyData() = {
      // discrepancy info
      // thinking about pulling all of these paths in to config? in case versions change?
      val svb = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendorbidding/v=1/"
      val svbCols = Seq("RequestName" , "SupplyVendor" , "SupplyVendorId" , "DiscrepancyAdjustment")
      val svbDf = getParquetData(date, 1, svb, svbCols)
        .withColumn("svbDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
        .drop("DiscrepancyAdjustment")

      // thinking about pulling all of these paths in to config? in case versions change?
      val pdaPath = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/partnersupplyvendordiscrepancyadjustment/v=1/"
      val pdaCols = Seq("SupplyVendorName" , "SupplyVendor" , "PartnerId", "DiscrepancyAdjustment")
      val pdaDf = getParquetData(date, lookBack, pdaPath, pdaCols)
        .withColumn("pdaDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
        .drop("DiscrepancyAdjustment")

      // thinking about pulling all of these paths in to config? in case versions change?
      val dealsPath = f"s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendordeal/v=1/"
      val dealDf = getParquetData(date, lookBack, dealsPath)
        .join(svbDf, "SupplyVendorId")
        .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))

      (svbDf, pdaDf, dealDf)
    }

    // tentative output schema
    // case class Impressions(BidRequestId: String, DealId: String, MediaCostCPMInUSD: Double, RealMediaCostInUSD: Double, RealMediaCost: Double, DiscrepancyAdjustmentMultiplier: Double, i_RealBidPrice: Float, SubmittedBidAmountInUSD: Double, FirstPriceAdjustment: Double, imp_adjuster: Double )
    def getAdjustedImpressions(svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame) = {

      // thinking about pulling all of these paths in to config? in case versions change?
      val impsPath = f"s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=4/"

      val impressions = getParquetData(date, lookBack, impsPath)
        .filter(col("SupplyVendor") === svName)
        .filter(col("AuctionType") === "FirstPrice")
        .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))


      // Empirical Discrepancy from Impressions
      val empDisDf = impressions.alias("bf")
        .select(
          col("PartnerId"),
          col("SupplyVendor"),
          col("DealId"),
          col("AdFormat"),
          coalesce(col("DiscrepancyAdjustmentMultiplier"), lit(1)).alias("DiscrepancyAdjustmentMultiplier")
        )
        .groupBy("PartnerId", "SupplyVendor", "DealId", "AdFormat")
        .agg(round(avg("DiscrepancyAdjustmentMultiplier"), 2).as("EmpiricalDiscrepancy"))


      val impsDf = impressions.alias("bf")
        .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
        .join(broadcast(pdaDf).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
        .join(broadcast(svbDf).alias("svb"), Seq("SupplyVendor"), "left")
        .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

        .withColumn("imp_adjuster",
          coalesce(
            'FirstPriceAdjustment / 'DiscrepancyAdjustmentMultiplier,
            lit(1)/coalesce(
              'DiscrepancyAdjustmentMultiplier,
              'EmpiricalDiscrepancy,
              'pdaDiscrpancyAdjustment,
              'svbDiscrpancyAdjustment,
              lit(1)
            )
          )
        )
        .withColumn("RealMediaCostInUSD", 'MediaCostCPMInUSD / 'DiscrepancyAdjustmentMultiplier)
        .withColumn("RealMediaCost", round('RealMediaCostInUSD, 3))
        .withColumn("i_RealBidPriceInUSD", 'SubmittedBidAmountInUSD * 1000 * 'imp_adjuster)
        .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, 3))

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

      impressionsGauge.set(impsDf.count())

      (impsDf, empDisDf)

    }

    def getBidsData(svbDf: DataFrame, pdaDf: DataFrame, dealDf: DataFrame, empDisDf: DataFrame) = {
      val bidsPath = f"s3://ttd-datapipe-data/parquet/rtb_bidrequest_cleanfile/v=4/"
      println(bidsPath)

      val bidsDf = getParquetData(date, lookBack, bidsPath).alias("bids")
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
