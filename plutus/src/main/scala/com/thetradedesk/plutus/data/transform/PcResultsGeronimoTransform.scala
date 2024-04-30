package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.transform.PlutusDataTransform.{loadPlutusLogData, loadRawMbtwData}
import com.thetradedesk.plutus.data.transform.SharedTransforms.{AddChannel, AddMarketType}
import com.thetradedesk.plutus.data.{IMPLICIT_DATA_SOURCE, LossReason, loadParquetDataHourly, loadParquetDataHourlyV2}
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

  val spark = job.PcResultsGeronimoJob.spark
  import spark.implicits._

  def joinGeronimoPcResultsLog(geronimo: Dataset[BidsImpressionsSchema],
                               plutusLogs: Dataset[PlutusLogsData],
                               mbtw: Dataset[MinimumBidToWinData],
                               privateContractsData: Dataset[PrivateContractRecord],
                               adFormatData: Dataset[AdFormatRecord],
                               productionAdgroupBudgetData: Dataset[ProductionAdgroupBudgetData],
                               joinCols: Seq[String] = Seq("BidRequestId")
                              ): (Dataset[PcResultsMergedSchema], sql.DataFrame, sql.DataFrame) = {
    var res = geronimo.join(plutusLogs, joinCols, "left").join(mbtw, joinCols, "left")
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

    // Adding coalesced AliasedSupplyPublisherId + SupplyVendorPublisherId
    res = res.withColumn("AspSvpId",
        coalesce(
          when(col("AliasedSupplyPublisherId").isNotNull, concat(lit("asp_"), col("AliasedSupplyPublisherId"))),
          when(col("SupplyVendorPublisherId").isNotNull, concat(lit("svp_"), col("SupplyVendorPublisherId")))
        )
      )

    val pcResultsAbsentDataset = plutusLogs.join(geronimo, joinCols, "leftanti")
    val mbtwAbsentDataset = mbtw.join(geronimo, joinCols, "leftanti")

    (res.selectAs[PcResultsMergedSchema], pcResultsAbsentDataset, mbtwAbsentDataset)
  }


  def transform(dateTime: LocalDateTime, fileCount: Int) = {
    val geronimoDataset = loadParquetDataHourly[BidsImpressionsSchema](
      f"${BidsImpressions.BIDSIMPRESSIONSS3}prod/bidsimpressions",
      dateTime,
      source = Some(IMPLICIT_DATA_SOURCE)
    )

    val pcResultsDataset = loadPlutusLogData(dateTime)
    val mbtwData = loadRawMbtwData(dateTime)

    val privateContractsData = PrivateContractDataSet().readDate(dateTime.toLocalDate)
    val adFormatData = AdFormatDataSet().readDate(dateTime.toLocalDate)

    // This dataset contains multiple entries with different timestamps.
    // We use aggregation to extract the single entries we need
    // There might be some data lost in cases where these booleans change within
    // the hour but we dont think thats worth the additional computation needed
    // to join using a moving window.
    val productionAdgroupBudgetData = loadParquetDataHourlyV2[ProductionAdgroupBudgetData](
      ProductionAdgroupBudgetDataset.S3PATH,
      ProductionAdgroupBudgetDataset.S3PATH_GEN,
      dateTime
    ).groupBy("AdGroupId")
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

    val pcResultsAbsentCount = pcResultsAbsentDataset.count()
    val mbtwAbsentCount = mbtwAbsentDataset.count()

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    val outputPath = f"${PcResultsMergedDataset.S3PATH}${PcResultsMergedDataset.S3PATH_GEN.apply(dateTime)}"
    mergedDataset.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)

    val rows = listener.rowsWritten
    println(s"Rows Written: $rows")

    numRowsWritten.set(rows)
    numRowsAbsent.labels("pcResultsLog").set(pcResultsAbsentCount)
    numRowsAbsent.labels("mbtw").set(mbtwAbsentCount)
  }
}
