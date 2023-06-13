package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, loadModelFeatures}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object PlutusDataTransform extends Logger {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2

  def transform(date: LocalDate,
                svNames: Seq[String],
                partitions: Int,
                outputPath: String,
                dataVersion: Int,
                rawOutputPrefix: String,
                cleanOutputPrefix: String,
                implicitSampleRate: Double,
                inputTtdEnv: String,
                maybeOutputTtdEnv: Option[String] = None,
                featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {

    val bidsGaugeExplicit = prometheus.createGauge("raw_bids_count_exp", "count of raw bids (explicit)")
    val bidsGaugeImplicit = prometheus.createGauge("raw_bids_count_imp", "count of raw bids (implicit)")

    val mbtwData = loadMbtwData(date, svNames).repartition(partitions)
    log.info("lost bid data " + mbtwData.cache.count())


    val fpaBidsImpsExplicit = loadExplicitFpaBidsImpsData(date, svNames, inputTtdEnv)
    val fpaBidsImpsImplicit = loadImplicitFpaBidsImpsData(date, svNames, inputTtdEnv, implicitSampleRate)


    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
    val dealDf = dealData(svb, deals)

    val rawExplicit = fpaBidsImpsWithDiscrepancy(fpaBidsImpsExplicit, svb, pda, dealDf, empiricalImpressions(fpaBidsImpsExplicit), Some(mbtwData), partitions)
    val rawImplicit = fpaBidsImpsWithDiscrepancy(fpaBidsImpsImplicit, svb, pda, dealDf, empiricalImpressions(fpaBidsImpsImplicit), None, partitions)

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    bidsGaugeExplicit.set(rawExplicit.count())
    writeParquet(rawExplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "explicit", date)
    bidsGaugeImplicit.set(rawImplicit.count())
    writeParquet(rawImplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "implicit", date)


    // TODO: model features should be versioned
    val modelFeatures = loadModelFeatures(featuresJson)

    val outputTypes = Seq(cleanOutputPrefix, "csv")

    val cleanExplicitDataset = cleanExplicitData(rawExplicit).cache()
    outputTypes.foreach(x =>
      writeDatasetToS3(
        trainingDataset = cleanExplicitDataset,
        outputPath = versionedOutputPath,
        ttdEnv = maybeOutputTtdEnv.getOrElse(inputTtdEnv),
        outputPrefix = x,
        dataName = "explicit",
        date = date,
        maybeModelFeatures = Some(modelFeatures),
      )
    )

    val cleanImplicitDataset = cleanImplicitData(rawImplicit).cache()
    outputTypes.foreach(x =>
      writeDatasetToS3(
        trainingDataset = cleanImplicitDataset,
        outputPath = versionedOutputPath,
        ttdEnv = maybeOutputTtdEnv.getOrElse(inputTtdEnv),
        outputPrefix = x,
        dataName = "implicit",
        date = date,
        maybeModelFeatures = Some(modelFeatures),
      )
    )
  }

  def withAuctionBidPrice(df: DataFrame): DataFrame = {
    df.withColumn(
      "AuctionBidPrice",
      when(
        col("RealMediaCost").isNotNull, col("RealMediaCost")
      ).when(
        col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD")), col("FloorPriceInUSD")
      ).otherwise(
        col("b_RealBidPrice")
      )
    )
  }

  def cleanImplicitData(rawDf: DataFrame, maybeDownsampleNeg: Option[Double] = None, randomSeed: Int = 123): Dataset[TrainingData] = {
    maybeDownsampleNeg match {
      case Some(rate) => withAuctionBidPrice(rawDf).stat.sampleBy("IsImp", Map(true -> 1.0, false -> rate), randomSeed).selectAs[TrainingData]
      case None => withAuctionBidPrice(rawDf).selectAs[TrainingData]
    }

  }


  def cleanExplicitData(rawDf: DataFrame, extremeValueThreshold: Double = .8): Dataset[TrainingData] = {
    withAuctionBidPrice(rawDf).filter(col("mbtw").isNotNull)
      .withColumn("valid",
        // there are cases that dont make sense - we choose to remove these for simplicity while we investigate further.
        // impressions where mbtw < Media Cost and mbtw is above a floor (if present)
        when(
          (col("IsImp")) &&
            (col("mbtw") <= col("RealMediaCost")) &&
            ((col("mbtw") >= col("FloorPriceInUSD")) || col("FloorPriceInUSD").isNullOrEmpty), true)

          // Bids where mbtw is > bid price AND not an extreme value AND is above floor (if present)
          .when(
            (!col("IsImp")) &&
              (col("mbtw") > col("b_RealBidPrice")) &&
              (col("FloorPriceInUSD").isNullOrEmpty || (col("mbtw") >= col("FloorPriceInUSD"))) &&
              (round(col("b_RealBidPrice") / col("mbtw"), 1) > extremeValueThreshold) &&
              ((col("mbtw") > col("FloorPriceInUSD")) || (col("FloorPriceInUSD").isNullOrEmpty)), true)
          .otherwise(false)
      ).filter(
        col("valid") === true
      )
      .drop(
        "valid"
      ).selectAs[TrainingData]
  }

  private def loadFpaBidsImpsData(date: LocalDate, ttdEnv: String): Dataset[BidsImpressionsSchema] = {
    loadParquetData[BidsImpressionsSchema](
      s3path = BidsImpressions.BIDSIMPRESSIONSS3 + f"${ttdEnv}/bidsimpressions/",
      date = date,
      source = Some(PLUTUS_DATA_SOURCE)
    ).filter(
      col("AuctionType") === 1
    )
  }

  def loadExplicitFpaBidsImpsData(date: LocalDate, svNames: Seq[String], ttdEnv: String): Dataset[BidsImpressionsSchema] = {
    loadFpaBidsImpsData(date, ttdEnv).filter(
      col("SupplyVendor").isin(svNames: _*)
    )
  }

  def loadImplicitFpaBidsImpsData(date: LocalDate,
                                  svNames: Seq[String],
                                  ttdEnv: String,
                                  implicitSampleRate: Double,
                                  randomSeed: Int = 123,
                                  stratificationColumns: Seq[Column] = Seq(col("SupplyVendor"), col("IsImp")),
                                  stratificationColumnName: String = "stratify_col"
                                 ): Dataset[BidsImpressionsSchema] = {

    val implicitData = loadFpaBidsImpsData(date, ttdEnv).filter(
      !col("SupplyVendor").isin(svNames: _*)
    )

    val implicitDataWithStratification = implicitData.withColumn(
      stratificationColumnName,
      concat_ws("_", stratificationColumns: _*)
    )

    val fractions = implicitDataWithStratification.select(
      stratificationColumnName
    ).distinct().as[String].collect().map((_, implicitSampleRate)).toMap

    implicitDataWithStratification.stat.sampleBy(
      stratificationColumnName, fractions, randomSeed
    ).drop(stratificationColumnName).as[BidsImpressionsSchema]
  }

  private def writeDatasetToS3(trainingDataset: Dataset[TrainingData], outputPath: String, ttdEnv: String, outputPrefix: String, dataName: String, date: LocalDate, numParitions: Option[Int] = None, maybeModelFeatures: Option[Seq[ModelFeature]]): Unit = {
    (outputPrefix, dataName) match {
      case ("csv", "explicit") =>
        trainingDataset.select(
          intModelFeaturesCols(maybeModelFeatures.getOrElse(DEFAULT_MODEL_FEATURES)) ++ plutusTargetCols(EXPLICIT_MODEL_TARGETS): _*
        ).repartition(
          numParitions.getOrElse(DEFAULT_NUM_CSV_PARTITIONS)
        ).write.option(
          "header", "true"
        ).mode(
          SaveMode.Overwrite
        ).csv(
          s"$outputPath/$ttdEnv/$outputPrefix/$dataName/${explicitDatePart(date)}"
        )
      case ("csv", "implicit") =>
        trainingDataset.select(
          intModelFeaturesCols(maybeModelFeatures.getOrElse(DEFAULT_MODEL_FEATURES)) ++ plutusTargetCols(IMPLICIT_MODEL_TARGETS): _*
        ).repartition(
          numParitions.getOrElse(DEFAULT_NUM_CSV_PARTITIONS)
        ).write.option(
          "header", "true"
        ).mode(
          SaveMode.Overwrite
        ).csv(
          s"$outputPath/$ttdEnv/$outputPrefix/$dataName/${explicitDatePart(date)}"
        )
      case (_, _) =>
        trainingDataset
          .repartition(
            numParitions.getOrElse(DEFAULT_NUM_PARQUET_PARTITIONS)
          ).write.mode(
          SaveMode.Overwrite
        ).parquet(
          s"$outputPath/$ttdEnv/$outputPrefix/$dataName/${explicitDatePart(date)}"
        )
    }
  }

  def writeParquet(rawData: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, dataName: String, date: LocalDate): Unit = {
    // note the date part is year=yyyy/month=m/day=d/
    rawData
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(s"$outputPath/$ttdEnv/$outputPrefix/$dataName/${explicitDatePart(date)}")
  }

  def loadMbtwData(date: LocalDate, svNames: Seq[String]): Dataset[MinimumBidToWinData] = {
    loadCsvData[RawLostBidData](RawLostBidDataset.S3PATH, date, RawLostBidDataset.SCHEMA)
      .filter(col("SupplyVendor").isin(svNames: _*))
      // https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/DB/Provisioning/TTD.DB.Provisioning.Primitives/LossReason.cs
      .filter((col("LossReason") === LOSS_CODE_WIN) || (col("LossReason") === LOSS_CODE_LOST_TO_HIGHER_BIDDER))
      .filter(col("mbtw") =!= 0.0)
      .select(
        col("BidRequestId").cast(StringType),
        col("SupplyVendorLossReason").cast(IntegerType),
        col("LossReason").cast(IntegerType),
        col("WinCPM").cast(DoubleType),
        col("mbtw").cast(DoubleType)
      ).as[MinimumBidToWinData]
  }

  def loadPlutusLogData(date: LocalDate): Dataset[PlutusLogsData] = {
    loadParquetData[PlutusLogsData](PlutusLogsDataset.S3PATH, date, lookBack = Some(1))
      .select(
        "PlutusLog.*",
        "PredictiveClearingStrategy.*",
        "*"
      ).drop(
      "PlutusLog",
      "PredictiveClearingStrategy"
    ).as[PlutusLogsData]
  }

  def dealData(svb: Dataset[Svb], deals: Dataset[Deals]): DataFrame = {
    deals
      .join(svb.alias("svb"), "SupplyVendorId")
      .withColumn("SupplyVendor", col("svb.RequestName"))
      .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))
  }


  def empiricalImpressions(fpaBidsImpressions: Dataset[BidsImpressionsSchema]): Dataset[EmpiricalDiscrepancy] = {
    fpaBidsImpressions
      .filter(col("IsImp"))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .alias("bf")
      .select(
        col("PartnerId"),
        col("SupplyVendor"),
        col("DealId"),
        col("AdFormat"),
        coalesce(col("DiscrepancyAdjustmentMultiplier").cast(DoubleType), lit(1.0)).alias("DiscrepancyAdjustmentMultiplier")
      )
      .groupBy("PartnerId", "SupplyVendor", "DealId", "AdFormat")
      .agg(round(avg("DiscrepancyAdjustmentMultiplier").cast(DoubleType), EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION).cast(DoubleType).as("EmpiricalDiscrepancy")
      ).selectAs[EmpiricalDiscrepancy]
  }


  def fpaBidsImpsWithDiscrepancy(bidsImpressions: Dataset[BidsImpressionsSchema], svb: Dataset[Svb], pda: Dataset[Pda], dealDf: DataFrame, empDisDf: Dataset[EmpiricalDiscrepancy], maybeMbtw: Option[Dataset[MinimumBidToWinData]], partitions: Int): DataFrame = {
    val data = bidsImpressions
      .alias("bids")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))

      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pda.withColumn("SupplyVendor", col("SupplyVendorName"))).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svb).alias("svb"), col("SupplyVendor") === col("RequestName"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .drop("svb.RequestName")
      .drop("pda.SupplyVendorName")
      //      .repartition(partitions)

      // determining how much the bid was adjusted by to back out real bid price
      .withColumn("bid_adjuster",
        coalesce('BidsFirstPriceAdjustment,
          lit(1.0) / coalesce(
            col("EmpiricalDiscrepancy").cast(DoubleType),
            col("pda.DiscrepancyAdjustment").cast(DoubleType),
            col("svb.DiscrepancyAdjustment").cast(DoubleType),
            lit(1.0)
          )
        )
      )

      // same as above, figuring out all adjustments for in bidfeedback
      .withColumn("imp_adjuster",
        coalesce(
          'ImpressionsFirstPriceAdjustment / 'DiscrepancyAdjustmentMultiplier,
          lit(1.0) / coalesce(
            col("DiscrepancyAdjustmentMultiplier").cast(DoubleType),
            col("EmpiricalDiscrepancy").cast(DoubleType),
            col("pda.DiscrepancyAdjustment").cast(DoubleType),
            col("svb.DiscrepancyAdjustment").cast(DoubleType),
            lit(1.0)
          )
        )
      )

      // added a coalesced AliasedSupplyPublisherId and SupplyVendorPublisherId
      .withColumn("AspSvpId",
        coalesce(
          when(col("AliasedSupplyPublisherId").isNotNull, concat(lit("asp_"), col("AliasedSupplyPublisherId"))),
          when(col("SupplyVendorPublisherId").isNotNull, concat(lit("svp_"), col("SupplyVendorPublisherId")))
        )
      )

      .withColumn("RealMediaCostInUSD", col("MediaCostCPMInUSD").cast(DoubleType) / col("DiscrepancyAdjustmentMultiplier"))
      .withColumn("RealMediaCostInUSD", 'MediaCostCPMInUSD / 'DiscrepancyAdjustmentMultiplier)
      .withColumn("RealMediaCost", round('RealMediaCostInUSD, ROUNDING_PRECISION))
      .withColumn("i_RealBidPriceInUSD", col("SubmittedBidAmountInUSD").cast(DoubleType) * 1000 * col("imp_adjuster"))
      .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, ROUNDING_PRECISION))

      .withColumn("b_RealBidPriceInUSD", col("AdjustedBidCPMInUSD").cast(DoubleType) * col("bid_adjuster"))
      .withColumn("b_RealBidPrice", round('b_RealBidPriceInUSD, ROUNDING_PRECISION))
      // .withColumn("PredictiveClearingRandomControl", when(col("PredictiveClearingRandomControl"), 1).otherwise(0))

      // only select variable priced deals
      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .withColumn(
        "AuctionBidPrice",
        when(
          col("RealMediaCost").isNotNull, col("RealMediaCost")
        ).when(
          (col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD"))), col("FloorPriceInUSD")
        ).otherwise(col("b_RealBidPrice"))
      )
      .select(

        col("BidRequestId"),
        col("DealId"),

        // adjusted impression cols

        col("MediaCostCPMInUSD").cast(DoubleType).alias("MediaCostCPMInUSD"),
        col("RealMediaCostInUSD").cast(DoubleType).alias("RealMediaCostInUSD"),
        col("RealMediaCost").cast(DoubleType).alias("RealMediaCost"),
        col("DiscrepancyAdjustmentMultiplier").cast(DoubleType).alias("DiscrepancyAdjustmentMultiplier"),

        col("i_RealBidPrice").cast(DoubleType),
        (col("SubmittedBidAmountInUSD") * 1000).cast(DoubleType).alias("ImpressionsOriginalBidPrice"),
        col("ImpressionsFirstPriceAdjustment").cast(DoubleType).alias("ImpressionsFirstPriceAdjustment"),
        col("imp_adjuster").cast(DoubleType),

        col("AdjustedBidCPMInUSD").cast(DoubleType).alias("AdjustedBidCPMInUSD"),
        col("BidsFirstPriceAdjustment").cast(DoubleType),
        col("FloorPriceInUSD").cast(DoubleType).alias("FloorPriceInUSD"),


        // calculated values
        col("b_RealBidPriceInUSD").cast(DoubleType).alias("b_RealBidPriceInUSD"),
        col("b_RealBidPrice").cast(DoubleType).alias("b_RealBidPrice"),
        col("bid_adjuster").cast(DoubleType).alias("bid_adjuster"),


        // Identifiers
        col("PartnerId"),
        col("AdvertiserId"),
        col("CampaignId"),
        col("AdGroupId"),


        // Contextual
        col("SupplyVendor"),
        col("SupplyVendorPublisherId"),
        col("AliasedSupplyPublisherId"),
        col("AspSvpId"),
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
        col("IsImp")
      )

    maybeMbtw match {
      case Some(mbtw) => data.join(mbtw, Seq("BidRequestId"), "left").repartition(partitions)
      case None => data.withColumn("mbtw", lit(null).cast(DoubleType)).repartition(partitions)
    }

  }
}
