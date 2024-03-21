package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{intModelFeaturesCols, loadModelFeatures}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions, DataSetExtensions}
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}

import java.time.{LocalDate, LocalDateTime}

object PlutusDataTransform extends Logger {

  val ROUNDING_PRECISION = 3
  val EMPIRICAL_DISCREPANCY_ROUNDING_PRECISION = 2
  val DEFAULT_MIN_BID = 0.01
  val DEFAULT_MAX_BID = 300.0

  def processExplicit(date: LocalDate,
                      partitions: Int,
                      outputPath: String,
                      dataVersion: Int,
                      rawOutputPrefix: String,
                      cleanOutputPrefix: String,
                      inputTtdEnv: String,
                      maybeOutputTtdEnv: Option[String] = None,
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {


    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
    val dealDf = dealData(svb, deals)

    val (mbtwData, mbtwSv) = loadMbtwData(date)
    val fpaBidsImpsExplicit = loadExplicitFpaBidsImpsData(date = date, svNames = mbtwSv, ttdEnv = inputTtdEnv)

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val rawExplicit = fpaBidsImpsMbtwDiscrepancy(
      bidsImpressions = fpaBidsImpsExplicit,
      svb = svb,
      pda = pda,
      dealDf = dealDf,
      empDisDf = empiricalDiscrepancy(fpaBidsImpsExplicit),
      maybePartitions = Some(partitions),
      maybeMbtw = Some(mbtwData)
    )
    val rawParquetPath = writeParquet(rawExplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "explicit", date)

    // its sometimes faster to read in the data from s3 rather than wait for the full dag
    val rawExplicitData = spark.read.parquet(rawParquetPath).as[PlutusRawDataset]

    // should just check that the data contains these rather than filtering to this list
    val modelFeatures = loadModelFeatures(featuresJson)
    println(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES))
    println(checkColumnCoverage(modelFeatures, rawExplicitData.columns))
    //    assert(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES), "Feature Cols not present in Data Cols")
    //    assert(checkColumnCoverage(modelFeatures, rawExplicitData.columns), s"Missing Feature Columns in Data ${modelFeatures.map(_.name)} ${rawExplicitData.columns}")

    val bidsGaugeExplicit = prometheus.createGauge("raw_bids_count_exp", "count of raw bids (explicit)")
    bidsGaugeExplicit.set(rawExplicitData.count())

    val cleanExplicitDataset = cleanExplicitData(rawExplicitData).repartition(partitions).persist()

    mbtwSv.foreach {
      sv =>
        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/${cleanOutputPrefix}/$sv/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(intModelFeaturesCols(PlutusTrainingDataset.DATA_FEATURES) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
          .na.fill(0)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/${cleanOutputPrefix}-hashed/$sv/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(hashedModMaxIntFeaturesCols(PlutusTrainingDataset.DATA_FEATURES, DEFAULT_SHIFT) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
          .na.fill(0)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/${cleanOutputPrefix}-hashed-mod-maxint/$sv/${explicitDatePart(date)}")

        cleanExplicitDataset
          .filter(col("SupplyVendor") === sv)
          .select(intModelFeaturesCols(PlutusTrainingDataset.DATA_FEATURES) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
          .na.fill(0)
          .write
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/explicit/csv/$sv/${explicitDatePart(date)}")
    }

    cleanExplicitDataset.unpersist()

  }

  def checkColumnCoverage(modelCols: Seq[ModelFeature], dataCols: Array[String]): Seq[String] = {
    modelCols.map(_.name.toLowerCase).diff(dataCols.map(_.toLowerCase))
    //    modelCols.map(c => c.name).map(c => dataCols.contains(c)).forall(_ == true)
  }

  def checkFeatureCoverage(modelCols: Seq[ModelFeature], dataCols: Seq[ModelFeature]): Seq[String] = {
    dataCols.map(_.name.toLowerCase).diff(modelCols.map(_.name.toLowerCase))
    //    modelCols.map(_.name).map(dataCols.map(_.name).contains(_)).forall(_ == true)
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
                      featuresJson: String)(implicit prometheus: PrometheusClient): Unit = {


    val svb = loadParquetData[Svb](DiscrepancyDataset.SBVS3, date)
    val pda = loadParquetData[Pda](DiscrepancyDataset.PDAS3, date)
    val deals = loadParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
    val dealDf = dealData(svb, deals)

    val fpaBidsImpsImplicit = loadImplicitFpaBidsImpsData(date = date, ttdEnv = inputTtdEnv, implicitSampleRate = implicitSampleRate, filterInvalidLoss = true)

    val rawImplicit = fpaBidsImpsMbtwDiscrepancy(
      bidsImpressions = fpaBidsImpsImplicit,
      svb = svb,
      pda = pda,
      dealDf = dealDf,
      empDisDf = empiricalDiscrepancy(fpaBidsImpsImplicit),
      maybePartitions = Some(partitions)
    )

    val versionedOutputPath = s"$outputPath/v=$dataVersion"
    val rawParquetPath = writeParquet(rawImplicit, versionedOutputPath, maybeOutputTtdEnv.getOrElse(inputTtdEnv), rawOutputPrefix, "implicit", date)

    val rawImplicitData = spark.read.parquet(rawParquetPath).as[PlutusRawDataset]
    val bidsGaugeImplicit = prometheus.createGauge("raw_bids_count_imp", "count of raw bids (implicit)")
    bidsGaugeImplicit.set(rawImplicitData.count())

    // should just check that the data contains these rather than filtering to this list
    val modelFeatures = loadModelFeatures(featuresJson)
    println(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES))
    println(checkColumnCoverage(modelFeatures, rawImplicitData.columns))
    //    assert(checkFeatureCoverage(modelFeatures, PlutusTrainingDataset.DATA_FEATURES), "Feature Cols not present in Data Cols")
    //    assert(checkColumnCoverage(modelFeatures, rawImplicitData.columns), s"Missing Feature Columns in Data ${modelFeatures.map(_.name)} ${rawImplicitData.columns}")

    val cleanImplicitDataset = cleanImplicitData(rawImplicitData).repartition(partitions).persist()

    cleanImplicitDataset
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/${cleanOutputPrefix}/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(intModelFeaturesCols(PlutusTrainingDataset.DATA_FEATURES) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
      .na.fill(0)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/${cleanOutputPrefix}-hashed/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(hashedModMaxIntFeaturesCols(PlutusTrainingDataset.DATA_FEATURES, DEFAULT_SHIFT) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
      .na.fill(0)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/${cleanOutputPrefix}-hashed-mod-maxint/${explicitDatePart(date)}")

    cleanImplicitDataset
      .select(intModelFeaturesCols(PlutusTrainingDataset.DATA_FEATURES) ++ modelTargeCols(PlutusTrainingDataset.DATA_TARGETS): _*)
      .na.fill(0)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(s"$versionedOutputPath/${maybeOutputTtdEnv.getOrElse(inputTtdEnv)}/implicit/csv/${explicitDatePart(date)}")

    cleanImplicitDataset.unpersist()
  }


  def filterVeryLargeCPM(rawDs: Dataset[PlutusRawDataset], maybeMaxBid: Option[Double] = None, maybeMinBid: Option[Double] = None): Dataset[PlutusRawDataset] = {
    rawDs.filter(
      (col("AuctionBidPrice") < maybeMaxBid.getOrElse(DEFAULT_MAX_BID))
        && (col("AuctionBidPrice") > maybeMinBid.getOrElse(DEFAULT_MIN_BID))
    )
  }

  def cleanImplicitData(rawDs: Dataset[PlutusRawDataset], maybeDownsampleNeg: Option[Double] = None, randomSeed: Int = 123): Dataset[PlutusTrainingDataset] = {
    val df = filterVeryLargeCPM(rawDs)
    maybeDownsampleNeg match {
      case Some(rate) => df.stat.sampleBy("IsImp", Map(true -> 1.0, false -> rate), randomSeed).selectAs[PlutusTrainingDataset]
      case None => df.selectAs[PlutusTrainingDataset]
    }

  }


  def cleanExplicitData(rawDataset: Dataset[PlutusRawDataset], extremeValueThreshold: Double = .8): Dataset[PlutusTrainingDataset] = {
    rawDataset
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
              (col("mbtw") > col("AuctionBidPrice")) &&
              (col("FloorPriceInUSD").isNullOrEmpty || (col("mbtw") >= col("FloorPriceInUSD"))) &&
              (round(col("AuctionBidPrice") / col("mbtw"), 1) > extremeValueThreshold) &&
              ((col("mbtw") > col("FloorPriceInUSD")) || (col("FloorPriceInUSD").isNullOrEmpty)), true)
          .otherwise(false)
      ).filter(
        col("valid") === true
      )
      .drop(
        "valid"
      ).selectAs[PlutusTrainingDataset]
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

  def writeParquet(rawData: Dataset[_], outputPath: String, ttdEnv: String, outputPrefix: String, labelType: String, date: LocalDate): String = {
    // note the date part is year=yyyy/month=m/day=d/
    rawData
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")
      .parquet(s"$outputPath/$ttdEnv/$labelType/$outputPrefix/${explicitDatePart(date)}")

    s"$outputPath/$ttdEnv/$labelType/$outputPrefix/${explicitDatePart(date)}"
  }

  def loadRawMbtwData(dateTime: LocalDateTime): (Dataset[MinimumBidToWinData]) = {
    val rawMbtwData = loadCsvData[RawLostBidData](
      RawLostBidDataset.S3PATH,
      RawLostBidDataset.S3PATH_GEN,
      dateTime,
      RawLostBidDataset.SCHEMA
    )

    rawMbtwData.select(
      col("BidRequestId").cast(StringType),
      col("SupplyVendorLossReason").cast(IntegerType),
      col("LossReason").cast(IntegerType),
      col("WinCPM").cast(DoubleType),
      col("mbtw").cast(DoubleType)
    ).as[MinimumBidToWinData]
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
    loadParquetData[PcResultsRawLogSchema](PlutusLogsDataset.S3PATH, date, lookBack = Some(1))
      .select(
        "PlutusLog.*",
        "PredictiveClearingStrategy.*",
        "*"
      ).drop(
        "PlutusLog",
        "PredictiveClearingStrategy"
      ).as[PlutusLogsData]
  }

  def loadPlutusLogData(dateTime: LocalDateTime): Dataset[PlutusLogsData] = {
    loadParquetDataHourlyV2[PcResultsRawLogSchema](
      PlutusLogsDataset.S3PATH,
      PlutusLogsDataset.S3PATH_GEN,
      dateTime
    ).select(
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


  def fpaBidsImpsMbtwDiscrepancy(
                                  bidsImpressions: Dataset[BidsImpressionsSchema],
                                  svb: Dataset[Svb],
                                  pda: Dataset[Pda],
                                  dealDf: DataFrame,
                                  empDisDf: Dataset[EmpiricalDiscrepancy],
                                  maybePartitions: Option[Int] = None,
                                  maybeMbtw: Option[Dataset[MinimumBidToWinData]] = None,
                                ): Dataset[PlutusRawDataset] = {
    val df = bidsImpressions
      .alias("bids")
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))

      .join(empDisDf.alias("ed"), Seq("PartnerId", "SupplyVendor", "DealId", "AdFormat"), "left")
      .join(broadcast(pda.withColumn("SupplyVendor", col("SupplyVendorName"))).alias("pda"), Seq("PartnerId", "SupplyVendor"), "left")
      .join(broadcast(svb).alias("svb"), col("SupplyVendor") === col("RequestName"), "left")
      .join(broadcast(dealDf).alias("deal"), Seq("SupplyVendor", "DealId"), "left")

      .drop("svb.RequestName")
      .drop("pda.SupplyVendorName")

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
      .withColumn("RealMediaCostInUSD", (col("MediaCostCPMInUSD") / col("DiscrepancyAdjustmentMultiplier")).cast(DoubleType))
      .withColumn("RealMediaCost", round('RealMediaCostInUSD, ROUNDING_PRECISION).cast(DoubleType))
      .withColumn("i_RealBidPriceInUSD", col("SubmittedBidAmountInUSD").cast(DoubleType) * 1000 * col("imp_adjuster"))
      .withColumn("i_RealBidPrice", round('i_RealBidPriceInUSD, ROUNDING_PRECISION))

      .withColumn("b_RealBidPriceInUSD", col("AdjustedBidCPMInUSD").cast(DoubleType) * col("bid_adjuster"))
      .withColumn("b_RealBidPrice", round('b_RealBidPriceInUSD, ROUNDING_PRECISION).cast(DoubleType))

      // cast to double from BigDecimal
      .withColumn("AdjustedBidCPMInUSD", col("AdjustedBidCPMInUSD").cast(DoubleType))
      .withColumn("FloorPriceInUSD", col("FloorPriceInUSD").cast(DoubleType))
      .withColumn("PredictiveClearingMode", col("PredictiveClearingMode.value").cast(IntegerType))

      // extract value from records
      .withColumn("DeviceType", col("DeviceType.value").cast(IntegerType))
      .withColumn("OperatingSystemFamily", col("OperatingSystemFamily.value").cast(IntegerType))
      .withColumn("Browser", col("Browser.value").cast(IntegerType))
      .withColumn("RenderingContext", col("RenderingContext.value").cast(IntegerType))
      .withColumn("AdsTxtSellerType", col("AdsTxtSellerType.value").cast(IntegerType))
      .withColumn("PublisherType", col("PublisherType.value").cast(IntegerType))


      // only select variable priced deals
      // This Filter does not make sense anymore since the "IsVariablePrice" is only suggestive and should not be used
      // Anything that is Auction type == 1 is valid training data.
      //      .filter(col("DealId").isNullOrEmpty || col("IsVariablePrice") === true)

      .withColumn(
        "AuctionBidPrice",
        when(
          col("RealMediaCost").isNotNull, col("RealMediaCost")
        ).when(
          (col("FloorPriceInUSD").isNotNull && (col("b_RealBidPrice") < col("FloorPriceInUSD"))), col("FloorPriceInUSD")
        ).otherwise(col("b_RealBidPrice")).cast(DoubleType)
      )

    (maybeMbtw, maybePartitions) match {
      case (Some(mbtwDataset), Some(partitions)) => df.join(mbtwDataset, Seq("BidRequestId"), "inner").repartition(partitions).selectAs[PlutusRawDataset]
      case (Some(mbtwDataset), None) => df.join(mbtwDataset, Seq("BidRequestId"), "inner").selectAs[PlutusRawDataset]
      case (None, Some(partitions)) => df.withColumn("mbtw", lit(null).cast(DoubleType)).repartition(partitions).selectAs[PlutusRawDataset]
      case (None, None) => df.withColumn("mbtw", lit(null).cast(DoubleType)).selectAs[PlutusRawDataset]
    }
  }
}
