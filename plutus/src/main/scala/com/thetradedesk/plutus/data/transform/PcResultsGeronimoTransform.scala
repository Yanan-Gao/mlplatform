package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.transform.SharedTransforms.{AddChannel, AddMarketType}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdFormatDataSet, AdFormatRecord, PrivateContractDataSet, PrivateContractRecord}
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataFrameExtensions}
import job.PcResultsGeronimoJob.{numRowsAbsent, numRowsWritten}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDateTime

object PcResultsGeronimoTransform extends Logger {
  val delta = 0.0001
  val extremeValueThreshold = 0.75

  // These columns exist in Geronimo already
  val excludeFromPlutusLogsData = Seq("LogEntryTime","IsValuePacing","AuctionType","DealId","SupplyVendor","AdgroupId")

  def joinGeronimoPcResultsLog(geronimo: Dataset[BidsImpressionsSchema],
                               plutusLogs: Dataset[PlutusLogsData],
                               mbtw: Dataset[MinimumBidToWinData],
                               privateContractsData: Dataset[PrivateContractRecord],
                               adFormatData: Dataset[AdFormatRecord],
                               productionAdgroupBudgetData: Dataset[ProductionAdgroupBudgetData],
                               joinCols: Seq[String] = Seq("BidRequestId")
                              ): (Dataset[PcResultsMergedSchema], Dataset[PlutusLogsData], sql.DataFrame) = {

    var res = geronimo.join(plutusLogs.drop(excludeFromPlutusLogsData: _*), joinCols, "left")
      .join(mbtw, joinCols, "left")
      .withColumn("AdFormat", concat($"AdWidthInPixels", lit("x"), $"AdHeightInPixels"))

    res = AddChannel(res, adFormatData)
    res = AddMarketType(res, privateContractsData)

    // Converting values from BigDecimal to double to avoid the cost & complexity of BigDecimal
    // Because we dont need the decimal precision.
    res = res.withColumn("AdjustedBidCPMInUSD", $"AdjustedBidCPMInUSD".cast(DoubleType))
      .withColumn("BidsFirstPriceAdjustment", $"BidsFirstPriceAdjustment".cast(DoubleType))
      .withColumn("FloorPriceInUSD", $"FloorPriceInUSD".cast(DoubleType))
      .withColumn("MediaCostCPMInUSD", $"MediaCostCPMInUSD".cast(DoubleType))
      .withColumn("DiscrepancyAdjustmentMultiplier", $"DiscrepancyAdjustmentMultiplier".cast(DoubleType))
      .withColumn("AdvertiserCostInUSD", $"AdvertiserCostInUSD".cast(DoubleType))
      .withColumn("PartnerCostInUSD", $"PartnerCostInUSD".cast(DoubleType))
      .withColumn("TTDCostInUSD", $"TTDCostInUSD".cast(DoubleType))
      .withColumn("AdvertiserCurrencyExchangeRateFromUSD", $"AdvertiserCurrencyExchangeRateFromUSD".cast(DoubleType))
      .withColumn("SubmittedBidAmountInUSD", $"SubmittedBidAmountInUSD".cast(DoubleType))
      .withColumn("ImpressionsFirstPriceAdjustment", $"ImpressionsFirstPriceAdjustment".cast(DoubleType))
      .withColumn("ExpectedValue", $"ExpectedValue".cast(DoubleType))
      .withColumn("RPacingValue", $"RPacingValue".cast(DoubleType))


    res = res.withColumn("isMbtwValidStrict",
      // there are cases that dont make sense - we choose to remove these for simplicity while we investigate further.
      // impressions where MB2W < Media Cost and MB2W is above a floor (if present)
      when(
        (col("IsImp") === 1.0) && (col("LossReason") === LossReason.Win) &&
          (col("mbtw") <= col("FinalBidPrice")) &&
          ((col("mbtw") >= col("FloorPriceInUSD")) || col("FloorPriceInUSD").isNullOrEmpty), true)

        // Bids where MB2W is > bid price AND not an extreme value AND is above floor (if present)
        .when(
          (col("IsImp") === 0.0) && (col("LossReason") === LossReason.BidLostHigherBid) &&
            (col("mbtw") > col("FinalBidPrice")) &&
            (col("mbtw") >= col("FloorPriceInUSD") || col("FloorPriceInUSD").isNullOrEmpty) &&
            (col("FinalBidPrice") / col("mbtw") > extremeValueThreshold), true)
        .otherwise(false)
    )

    // A more flexible version of the above with a looser bound
    res = res.withColumn("isMbtwValid",
      when(
        (col("IsImp") === 1.0) && (col("LossReason") === LossReason.Win) &&
          ((col("mbtw") / col("FinalBidPrice")) < (1 + delta)) &&
          ((col("mbtw") / col("FloorPriceInUSD") > (1 - delta)) || col("FloorPriceInUSD").isNullOrEmpty), true)

        // Bids where MB2W is > bid price AND not an extreme value AND is above floor (if present)
        .when(
          (col("IsImp") === 0.0) && (col("LossReason") === LossReason.BidLostHigherBid) &&
            (col("mbtw") > col("FinalBidPrice")) &&
            ((col("mbtw") / col("FloorPriceInUSD") > (1 - delta)) || col("FloorPriceInUSD").isNullOrEmpty) &&
            ((col("FinalBidPrice") / col("mbtw")) > extremeValueThreshold), true)
        .otherwise(false)
    )

    // Getting Value Pacing fields from productionAdgroupBudgetData
    res = res.alias("df")
      .join(broadcast(productionAdgroupBudgetData).alias("agb"), ($"df.AdGroupId" === $"agb.AdGroupId"), "left")
      .select($"df.*", $"agb.IsValuePacing", $"agb.IsUsingPIDController")

    // Extracting Fee Feature
    res = res.withColumn("FeeAmount", expr("filter(FeeFeatureUsage, element -> element.FeeFeatureType.value == 41)[0].FeeAmount").cast(DoubleType))

    // Simplifying the dataset by removing structs which dont actually add meaningful structure.
    res = res.withColumn("DoNotTrack", col("DoNotTrack.value"))
      .withColumn("PredictiveClearingMode", col("PredictiveClearingMode.value"))
      .withColumn("InternetConnectionType", col("InternetConnectionType.value"))
      .withColumn("Browser", col("Browser.value"))
      .withColumn("OperatingSystemFamily", col("OperatingSystemFamily.value"))
      .withColumn("OperatingSystem", col("OperatingSystem.value"))
      .withColumn("DeviceType", col("DeviceType.value"))
      .withColumn("PublisherType", col("PublisherType.value"))
      .withColumn("AdsTxtSellerType", col("AdsTxtSellerType.value"))
      .withColumn("RenderingContext", col("RenderingContext.value"))

    // This is a temporary field while we figure out how to populate all the fields properly in a Janus world
    res = res.withColumn("IsUsingJanus", col("JanusVariantMap").isNotNull)

    // PlutusVersionUsed is parsed from the ModelVersionsUsed map column for plutus model name
    res = res.withColumn("PlutusVersionUsed",
      map_values(
        map_filter(col("ModelVersionsUsed"), (k, v) => k === "plutus")
      )(0) // null safe first element
    )

    // Adding coalesced AliasedSupplyPublisherId + SupplyVendorPublisherId
    res = res.withColumn("AspSvpId",
      coalesce(
        when(col("AliasedSupplyPublisherId").isNotNull, concat(lit("asp_"), col("AliasedSupplyPublisherId"))),
        when(col("SupplyVendorPublisherId").isNotNull, concat(lit("svp_"), col("SupplyVendorPublisherId")))
      )
    )

    val pcResultsAbsentDataset = plutusLogs.join(geronimo, joinCols, "leftanti")
      .as[PlutusLogsData]
    val mbtwAbsentDataset = mbtw.join(geronimo, joinCols, "leftanti")

    (res.selectAs[PcResultsMergedSchema], pcResultsAbsentDataset, mbtwAbsentDataset)
  }


  def transform(dateTime: LocalDateTime, fileCount: Int, ttdEnv: Option[String]): Unit = {
    val geronimoDataset = loadParquetDataHourly[BidsImpressionsSchema](
      f"${BidsImpressions.BIDSIMPRESSIONSS3}prod/bidsimpressions",
      dateTime,
      source = Some(IMPLICIT_DATA_SOURCE)
    )

    val pcResultsDataset = PlutusLogsData.loadPlutusLogData(dateTime)
    val mbtwData = MinimumBidToWinData.loadRawMbtwData(dateTime)

    val privateContractsData = PrivateContractDataSet().readLatestPartitionUpTo(dateTime.toLocalDate)
    val adFormatData = AdFormatDataSet().readLatestPartitionUpTo(dateTime.toLocalDate)

    // This dataset is used to label if a bid is value pacing or not (whether
    // the adgroup bidding uses DA or not). We're okay with slightly older data
    // This dataset contains multiple entries with different timestamps.
    // We use aggregation to extract the single entries we need
    // There might be some data lost in cases where these booleans change within
    // the hour but we dont think thats worth the additional computation needed
    // to join using a moving window.
    val productionAdgroupBudgetData = ProductionAdgroupBudgetDataset().readLatestPartitionUpToDateHour(dateTime, verbose = true, isInclusive = true)
      .groupBy("AdGroupId")
      .agg(
        max("IsValuePacing").alias("IsValuePacing"),
        max("IsUsingPIDController").alias("IsUsingPIDController")
      ).as[ProductionAdgroupBudgetData]

    val (mergedDataset, pcResultsAbsentDataset, mbtwAbsentDataset) = joinGeronimoPcResultsLog(geronimoDataset,
      pcResultsDataset,
      mbtwData,
      privateContractsData,
      adFormatData,
      productionAdgroupBudgetData)

    val mbtwAbsentCount = mbtwAbsentDataset.count()

    val plutusDataListener = new WriteListener()
    spark.sparkContext.addSparkListener(plutusDataListener)

    val plutusDataOutputPath = PcResultsMergedDataset.S3_PATH_HOUR(dateTime, ttdEnv)
    mergedDataset.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(plutusDataOutputPath)

    val rows = plutusDataListener.rowsWritten
    println(s"Rows Written: $rows")

    spark.sparkContext.removeSparkListener(plutusDataListener)
    numRowsWritten.set(rows)

    val optoutDataListener = new WriteListener()
    spark.sparkContext.addSparkListener(optoutDataListener)

    pcResultsAbsentDataset.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(PlutusOptoutBidsDataset.S3PATH_FULL_HOUR(dateTime, ttdEnv))

    numRowsAbsent.labels("pcResultsLog").set(optoutDataListener.rowsWritten)
    numRowsAbsent.labels("mbtw").set(mbtwAbsentCount)
  }
}
