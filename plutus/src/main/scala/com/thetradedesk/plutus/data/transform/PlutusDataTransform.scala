package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, loadModelFeatures}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark
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
                partitions: Int,
                outputPath: String,
                dataVersion: Int,
                rawOutputPrefix: String,
                cleanOutputPrefix: String,
                implicitSampleRate: Double,
                inputTtdEnv: String,
                maybeOutputTtdEnv: Option[String] = None,
                featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {

    val modelFeatures = loadModelFeatures(featuresJson)

    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
    val dealDf = dealData(svb, deals)


    processImplicit(
      date = date,
      partitions = partitions,
      outputPath = outputPath,
      dataVersion = dataVersion,
      implicitSampleRate = implicitSampleRate,
      rawOutputPrefix = rawOutputPrefix,
      cleanOutputPrefix = cleanOutputPrefix,
      inputTtdEnv = inputTtdEnv,
      maybeOutputTtdEnv = maybeOutputTtdEnv,
      modelFeatures = modelFeatures,
      svb = svb,
      pda = pda,
      dealDf = dealDf
    )

    processExplicit(
      date = date,
      partitions = 200, //partitions,
      outputPath = outputPath,
      dataVersion = dataVersion,
      rawOutputPrefix = rawOutputPrefix,
      cleanOutputPrefix = cleanOutputPrefix,
      inputTtdEnv = inputTtdEnv,
      maybeOutputTtdEnv = maybeOutputTtdEnv,
      modelFeatures = modelFeatures,
      svb = svb,
      pda = pda,
      dealDf = dealDf,
    )


  }

  def processExplicit(date: LocalDate,
                      partitions: Int,
                      outputPath: String,
                      dataVersion: Int,
                      rawOutputPrefix: String,
                      cleanOutputPrefix: String,
                      inputTtdEnv: String,
                      maybeOutputTtdEnv: Option[String] = None,
                      modelFeatures: Seq[ModelFeature],
                      svb: Dataset[Svb],
                      pda: Dataset[Pda],
                      dealDf: DataFrame)(implicit prometheus: PrometheusClient): Unit = {

    val (mbtwData, mbtwSv) = loadMbtwData(date)
    val fpaBidsImpsExplicit = loadExplicitFpaBidsImpsData(date = date, svNames = mbtwSv, ttdEnv = inputTtdEnv)

    var rawExplicit = fpaBidsImpsMbtwDiscrepancy(fpaBidsImpsExplicit, svb, pda, dealDf, empiricalDiscrepancy(fpaBidsImpsExplicit), Some(partitions))
      .join(mbtwData, Seq("BidRequestId"), "inner")
      .repartition(partitions)

    val bidsGaugeExplicit = prometheus.createGauge("raw_bids_count_exp", "count of raw bids (explicit)")
    bidsGaugeExplicit.set(rawExplicit.count())

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val rawParquetPath = writeParquet(rawExplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "explicit", date)

    rawExplicit = spark.read.parquet(rawParquetPath)

    val cleanExplicitDataset = cleanExplicitData(rawExplicit).cache()

    mbtwSv.foreach {
      case sv =>
        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(modelFeatureCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
          .repartition(partitions)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/clean/$sv/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(intModelFeaturesCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
          .repartition(partitions)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/clean-hashed/$sv/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(intModelFeaturesCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
          .repartition(partitions)
          .write
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/csv/$sv/${explicitDatePart(date)}")
    }

    cleanExplicitDataset.unpersist()

  }

  def processImplicit(date: LocalDate,
                      partitions: Int,
                      outputPath: String,
                      dataVersion: Int,
                      implicitSampleRate: Double,
                      rawOutputPrefix: String,
                      cleanOutputPrefix: String,
                      inputTtdEnv: String,
                      maybeOutputTtdEnv: Option[String] = None,
                      modelFeatures: Seq[ModelFeature],
                      svb: Dataset[Svb],
                      pda: Dataset[Pda],
                      dealDf: DataFrame)(implicit prometheus: PrometheusClient): Unit = {

    val fpaBidsImpsImplicit = loadImplicitFpaBidsImpsData(date = date, ttdEnv = inputTtdEnv, implicitSampleRate = implicitSampleRate, filterInvalidLoss = true)
//      .repartition(partitions)
    var rawImplicit = fpaBidsImpsMbtwDiscrepancy(fpaBidsImpsImplicit, svb, pda, dealDf, empiricalDiscrepancy(fpaBidsImpsImplicit), Some(partitions))
      .withColumn("mbtw", lit(null).cast(DoubleType))

//    val bidsGaugeImplicit = prometheus.createGauge("raw_bids_count_imp", "count of raw bids (implicit)")
//    bidsGaugeImplicit.set(rawImplicit.count())

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val rawParquetPath = writeParquet(rawImplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "implicit", date)

    rawImplicit = spark.read.parquet(rawParquetPath)

    val cleanImplicitDataset = cleanImplicitData(rawImplicit).cache()

    cleanImplicitDataset
      .select(modelFeatureCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/clean/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(intModelFeaturesCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/clean-hashed/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(intModelFeaturesCols(modelFeatures) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS): _*)
      .repartition(partitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/csv/${explicitDatePart(date)}")

    cleanImplicitDataset.unpersist()
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
    withAuctionBidPrice(rawDf)
      .filter(col("mbtw").isNotNull)
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
    loadFpaBidsImpsData(
      date = date,
      ttdEnv = ttdEnv,
    ).filter(
      col("SupplyVendor").isin(svNames: _*)
    )
  }

  def loadImplicitFpaBidsImpsData(date: LocalDate,
                                  ttdEnv: String,
                                  implicitSampleRate: Double,
                                  filterInvalidLoss: Boolean = true,
                                  randomSeed: Int = 123,
                                  stratificationColumns: Seq[Column] = Seq(col("SupplyVendor"), col("IsImp")),
                                  stratificationColumnName: String = "stratify_col"
                                 ): Dataset[BidsImpressionsSchema] = {


    val bidsImps =
      filterInvalidLoss match {
        case true =>
          val invalidLoss = loadInvalidLossData(date)
          loadFpaBidsImpsData(date = date, ttdEnv = ttdEnv)
            .join(invalidLoss, Seq("BidRequestId"), "left")
            .filter(col("IsInvalidLoss").isNull)
            .drop("IsInvalidLoss")

        case false =>
          loadFpaBidsImpsData(date = date, ttdEnv = ttdEnv)
      }

    val implicitData = bidsImps.withColumn(
      stratificationColumnName,
      concat_ws("_", stratificationColumns: _*)
    )

    val fractions = implicitData.select(
      stratificationColumnName
    ).distinct().as[String].collect().map((_, implicitSampleRate)).toMap

    implicitData.stat.sampleBy(
      stratificationColumnName, fractions, randomSeed
    ).drop(stratificationColumnName).as[BidsImpressionsSchema]
  }

  private def writeDatasetToS3(trainingDataset: Dataset[TrainingData], outputPath: String, ttdEnv: String,
                               outputPrefix: String, labelType: String, date: LocalDate, maybeNumPartitions: Option[Int] = None,
                               maybeModelFeatures: Option[Seq[ModelFeature]], hashedCols: Boolean = false): Unit = {


    def fullOutputPath(hashedCols: Boolean): String = {
      hashedCols match {
        case true => s"$outputPath/$ttdEnv/$outputPrefix/$labelType/hashed/${explicitDatePart(date)}"
        case false => s"$outputPath/$ttdEnv/$outputPrefix/$labelType/${explicitDatePart(date)}"

      }
    }

    def selectColumns(dataName: String, hashed: Boolean): Array[Column] = {
      (dataName, hashed) match {
        case ("explicit", true) => intModelFeaturesCols(maybeModelFeatures.getOrElse(DEFAULT_MODEL_FEATURES)) ++ modelTargeCols(EXPLICIT_MODEL_TARGETS)
        case ("implicit", true) => intModelFeaturesCols(maybeModelFeatures.getOrElse(DEFAULT_MODEL_FEATURES)) ++ modelTargeCols(IMPLICIT_MODEL_TARGETS)
        case (_, false) => modelFeatureCols(maybeModelFeatures.getOrElse(DEFAULT_MODEL_FEATURES)) ++ modelTargeCols(IMPLICIT_MODEL_TARGETS)
      }
    }

    (outputPrefix, labelType) match {

      case ("csv", "implicit") =>
        trainingDataset.select(
          selectColumns(labelType, hashedCols): _*
        ).repartition(
          maybeNumPartitions.getOrElse(DEFAULT_NUM_CSV_PARTITIONS)
        ).write.option(
          "header", "true"
        ).mode(
          SaveMode.Overwrite
        ).csv(
          fullOutputPath(hashedCols)
        )

      case ("csv", "explicit") =>
        trainingDataset.select(
          selectColumns(labelType, hashedCols): _*
        ).withColumn(
          "sv",
          col("SupplyVendor")
        ).repartition(
          maybeNumPartitions.getOrElse(DEFAULT_NUM_CSV_PARTITIONS)
        ).write.option(
          "header", "true"
        ).partitionBy(
          "sv"
        ).mode(
          SaveMode.Overwrite
        ).csv(
          fullOutputPath(hashedCols)
        )
      case (_, "explicit") =>
        trainingDataset.select(
          selectColumns(labelType, hashedCols): _*
        ).withColumn(
          "sv",
          col("SupplyVendor")
        ).repartition(
          maybeNumPartitions.getOrElse(DEFAULT_NUM_PARQUET_PARTITIONS)
        ).write.partitionBy(
          "sv"
        ).mode(
          SaveMode.Overwrite
        ).parquet(
          fullOutputPath(hashedCols)
        )
      case (_, _) =>
        trainingDataset.select(
          selectColumns(labelType, hashedCols): _*
        ).repartition(
          maybeNumPartitions.getOrElse(DEFAULT_NUM_PARQUET_PARTITIONS)
        ).write.mode(
          SaveMode.Overwrite
        ).parquet(
          fullOutputPath(hashedCols)
        )
    }
  }

  def writeParquet(rawData: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, labelType: String, date: LocalDate): String = {
    // note the date part is year=yyyy/month=m/day=d/
    rawData
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(s"$outputPath/$ttdEnv/$labelType/$outputPrefix/${explicitDatePart(date)}")

    s"$outputPath/$ttdEnv/$labelType/$outputPrefix/${explicitDatePart(date)}"
  }

  def loadMbtwData(date: LocalDate): (Dataset[MinimumBidToWinData], Seq[String]) = {
    val rawMbtwData = loadCsvData[RawLostBidData](RawLostBidDataset.S3PATH, date, RawLostBidDataset.SCHEMA)
      .filter(col("mbtw") =!= 0.0)

    val mbtwData = rawMbtwData.select(
      col("BidRequestId").cast(StringType),
      col("SupplyVendorLossReason").cast(IntegerType),
      col("LossReason").cast(IntegerType),
      col("WinCPM").cast(DoubleType),
      col("mbtw").cast(DoubleType)
    ).as[MinimumBidToWinData]

    val mbtwSupplyVendors = rawMbtwData.select("SupplyVendor").distinct().as[String].collect().toSeq

    (mbtwData, mbtwSupplyVendors)
  }

  def loadInvalidLossData(date: LocalDate): Dataset[InvalidLossData] = {
    loadCsvData[RawLostBidData](RawLostBidDataset.S3PATH, date, RawLostBidDataset.SCHEMA)
      .filter(!col("LossReason").isin(VALID_LOSS_CODES: _*))
      .select(
        col("BidRequestId").cast(StringType),
        lit(true).alias("IsInvalidLoss")
      ).as[InvalidLossData]
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

  def empiricalDiscrepancy(fpaBidsImpressions: Dataset[BidsImpressionsSchema]): Dataset[EmpiricalDiscrepancy] = {
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


  def fpaBidsImpsMbtwDiscrepancy(bidsImpressions: Dataset[BidsImpressionsSchema], svb: Dataset[Svb], pda: Dataset[Pda], dealDf: DataFrame, empDisDf: Dataset[EmpiricalDiscrepancy], maybePartitions: Option[Int] = None): DataFrame = {
    val df = bidsImpressions
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

    maybePartitions match {
      case Some(partitions) => df.repartition(partitions)
      case None => df
    }
  }
}
