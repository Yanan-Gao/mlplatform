package com.thetradedesk.plutus.data.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.plutus.data.{AuctionType, ChannelType, DeviceTypeId, IMPLICIT_DATA_SOURCE, MarketType, MediaTypeId, PublisherRelationshipType, RenderingContext, loadParquetDataHourly}
import com.thetradedesk.spark.datasets.sources.{AdFormatDataSet, AdFormatRecord, PrivateContractDataSet, PrivateContractRecord}
import com.thetradedesk.spark.util.LocalParquet
import com.thetradedesk.spark.util.TTDConfig.environment
import job.ModelPromotion.{spark, timestamp, trainBucket}
import job.ModelPromotion.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

case class ModelPromotionStats(PlutusTfModel: Option[String], totalBids: Long, totalWins: Long, totalSavings: Double, totalAdjustments: Double, totalSpend: Double) {
  val savingsRate = totalSavings / (totalSpend + totalSavings)
  val winRate = totalWins.toDouble / totalBids
  val realizedSurplusNorm = totalSavings / totalBids.toDouble
  val potentialSurplusNorm = totalAdjustments / totalBids.toDouble
}

case class ModelPromotionDetailedStats(PlutusTfModel: Option[String],
                                       Channel: Option[String],
                                       SupplyVendor: Option[String],
                                       DetailedMarketType: Option[String],
                                       totalBids: Long,
                                       totalWins: Long,
                                       totalAdjustments: Double,
                                       totalAvailable: BigDecimal,
                                       totalSavings: Double,
                                       totalSpend: Double)


object ModelPromotionTransform {

  val PATH_DETAILED_CSV: String = "detailed_results"

  def Transform(endDateTime: LocalDateTime, lookback: Int, outputPath: String): Array[ModelPromotionStats] = {

    val datapath = if (environment == LocalParquet) {
      "local-data/"
    } else {
      BidsImpressions.BIDSIMPRESSIONSS3
    }

    val startDateTime = endDateTime.minusHours(lookback)

    val bidsImpressionsData = loadParquetDataHourly[BidsImpressionsSchema](
      f"${datapath}prod/bidsimpressions",
      endDateTime,
      source = Some(IMPLICIT_DATA_SOURCE),
      lookBack = Some(lookback)
    ).where($"LogEntryTime" >= Timestamp.from(startDateTime.toInstant(ZoneOffset.UTC))
        and $"LogEntryTime" <= Timestamp.from(endDateTime.toInstant(ZoneOffset.UTC))
        and $"PredictiveClearingMode.value" === 3)


    val privateContractsData = PrivateContractDataSet().readDate(endDateTime.toLocalDate)
    val adFormatData = AdFormatDataSet().readDate(endDateTime.toLocalDate)

    // Detailed stats
    val detailedQuery = getDetailedStats(bidsImpressionsData, privateContractsData, adFormatData).cache()

    val detailed_output_path = s"$outputPath/${timestamp}_detailed_result/"

    detailedQuery.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header","true").csv(detailed_output_path)

    val simpleResults = detailedQuery.groupBy("PlutusTfModel")
      .agg(
        sum("totalBids").as("totalBids"),
        sum("totalWins").as("totalWins"),
        sum("totalAdjustments").as("totalAdjustments"),
        sum("totalAvailable").as("totalAvailable"),
        sum("totalSavings").as("totalSavings"),
        sum("totalSpend").as("totalSpend")
      ).as[ModelPromotionStats].collect()

    spark.close()
    simpleResults
  }


  def getDetailedStats(bidsImpressionsData: Dataset[BidsImpressionsSchema], privateContractsData: Dataset[PrivateContractRecord], adFormatData: Dataset[AdFormatRecord]) = {
    def AddMediaTypeFromAdFormat(df: DataFrame, af: Dataset[AdFormatRecord]): DataFrame = {
      df.alias("df")
        .join(broadcast(af).alias("af"), $"df.AdFormat" === $"af.DisplayNameShort", "left")
        .select($"df.*", $"MediaTypeId")
    }

    // Define the function get Channel
    def AddChannel(df: DataFrame, mediaTypeCol: String = "MediaTypeId", renderingContextCol: String = "RenderingContext.value", deviceTypeCol: String = "DeviceType.value"): DataFrame = {
      AddMediaTypeFromAdFormat(df, adFormatData)
        .withColumn("Channel",
          when(col(mediaTypeCol) === lit(MediaTypeId.Audio), ChannelType.Audio)
            .when(col(mediaTypeCol) === lit(MediaTypeId.Native), ChannelType.Native)
            .when(col(mediaTypeCol) === lit(MediaTypeId.NativeVideo), ChannelType.NativeVideo)
            .when(col(mediaTypeCol) === lit(MediaTypeId.Display),
              when(col(renderingContextCol) === lit(RenderingContext.MobileOptimizedWeb), ChannelType.MobileOptimizedWeb)
                .when(col(deviceTypeCol).isin(lit(DeviceTypeId.PC), lit(DeviceTypeId.ConnectedTV), lit(DeviceTypeId.Roku), lit(DeviceTypeId.Other))
                  && col(renderingContextCol).isin(lit(RenderingContext.Other), lit(RenderingContext.InApp)), ChannelType.Display)
                .when(col(deviceTypeCol) === lit(DeviceTypeId.OutOfHome), ChannelType.DigitalOutOfHome)
                .when(col(renderingContextCol) === lit(RenderingContext.InApp), ChannelType.MobileInApp)
                .when(col(renderingContextCol).isin(lit(RenderingContext.Other), lit(null)), ChannelType.MobileStandardWeb)
                .otherwise(ChannelType.Display)
            )
            .when(col(mediaTypeCol) === lit(MediaTypeId.Video),
              when(col(renderingContextCol) === lit(RenderingContext.MobileOptimizedWeb), ChannelType.MobileVideoOptimizedWeb)
                .when(col(deviceTypeCol).isin(lit(DeviceTypeId.Other), lit(DeviceTypeId.PC))
                  && col(renderingContextCol) === lit(RenderingContext.InApp), ChannelType.Video)
                .when(col(deviceTypeCol).isin(lit(DeviceTypeId.Tablet), lit(DeviceTypeId.Mobile))
                  && col(renderingContextCol) === lit(RenderingContext.InApp), ChannelType.MobileVideoInApp)
                .when(col(deviceTypeCol).isin(lit(DeviceTypeId.ConnectedTV), lit(DeviceTypeId.Roku))
                  && col(renderingContextCol) === lit(RenderingContext.InApp), ChannelType.ConnectedTV)
                .when(col(deviceTypeCol).isin(lit(DeviceTypeId.ConnectedTV), lit(DeviceTypeId.Roku), lit(DeviceTypeId.Mobile), lit(DeviceTypeId.Tablet))
                  && col(renderingContextCol).isin(lit(RenderingContext.Other), lit(null)), ChannelType.MobileVideoStandard)
                .when(col(deviceTypeCol) === lit(DeviceTypeId.OutOfHome), ChannelType.DigitalOutOfHome)
                .otherwise(ChannelType.Video)
            )
            .otherwise(ChannelType.Unknown))
        .drop("AdFormat", "RenderingContext", "DeviceType", "MediaTypeId")
    }

    def AddMarketType(df: DataFrame): DataFrame = {
      df.alias("df")
        .join(broadcast(privateContractsData).alias("pc"), $"df.PrivateContractId" === $"pc.PrivateContractId", "left")
        .withColumn("DetailedMarketType",
          when(col("DealId").isNotNull,
            when(col("PublisherRelationshipTypeId") === lit(PublisherRelationshipType.Indirect),
              when(col("IsProgrammaticGuaranteedContract"), MarketType.ProgrammaticGuaranteed)
                .when(!col("IsProgrammaticGuaranteedContract"),
                  when(col("AuctionType") === lit(AuctionType.FixedPrice), MarketType.PrivateAuctionFixedPrice)
                    .when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), MarketType.PrivateAuctionVariablePrice)
                    .otherwise(MarketType.UnknownPMP))
                .otherwise(MarketType.Unknown))
              .when(col("PublisherRelationshipTypeId") === lit(PublisherRelationshipType.Direct),
                when(col("IsProgrammaticGuaranteedV2"), MarketType.DirectTagGuaranteed)
                  .when(!col("IsProgrammaticGuaranteedV2"), MarketType.DirectTagNonGuaranteedFixedPrice)
                  .otherwise(MarketType.Unknown))
              // todo: This is deprecated. Remove if it doesn't show up.
              .when(col("PublisherRelationshipTypeId") === lit(PublisherRelationshipType.IndirectFixedPrice), MarketType.AccuenFixedPrice)
              .otherwise(MarketType.UnknownPMP))
            .otherwise(MarketType.OpenMarket))
        .drop("PublisherRelationshipTypeId", "IsProgrammaticGuaranteedV2", "IsProgrammaticGuaranteedContract")
    }

    var res = bidsImpressionsData
      // Using BidsFirstPriceAdjustment instead of ImpressionsFirstPriceAdjustment
      // ImpressionsFirstPriceAdjustment =~ BidsFirstPriceAdjustment * DiscrepancyAdjustmentMultiplier
      .withColumn("savings",
        when($"isImp" === false, 0.0)
          .when($"ImpressionsFirstPriceAdjustment".isNull, 0.0)
          .otherwise($"SubmittedBidAmountInUSD" * (lit(1.0) - $"BidsFirstPriceAdjustment"))
      )
      // We use AdjustedBidCPMInUSD here because SubmittedBidAmountInUSD is null when the bid is not an impression
      .withColumn("adjustments", $"AdjustedBidCPMInUSD" / 1000 * (lit(1.0) - $"BidsFirstPriceAdjustment"))
      // Available pushdown tells us how much more pushdown is available for the model
      .withColumn("available", ($"MediaCostCPMInUSD" - $"FloorPriceInUSD") / 1000)
      // MediaCostCPMInUSD =~ AdjustedBidCPMInUSD * ImpressionsFirstPriceAdjustment
      // (or MediaCostCPM =~ SubmittedBidAmountInUSD * ImpressionsFirstPriceAdjustment * 1000)
      .withColumn("spend",
        when($"isImp" === false, 0.0)
          .when($"ImpressionsFirstPriceAdjustment".isNull, 0.0)
          .otherwise($"MediaCostCPMInUSD" / 1000)
      )
      .withColumn("AdFormat", concat($"AdWidthInPixels", lit("x"), $"AdHeightInPixels"))
      .groupBy("PlutusTfModel", "AuctionType", "DealId", "PrivateContractId", "RenderingContext", "AdFormat", "DeviceType", "SupplyVendor")
      .agg(
        count("*").as("totalBids"),
        sum(col("isimp").cast("long")).as("totalWins"),
        sum("adjustments").as("totalAdjustments"),
        sum("available").as("totalAvailable"),
        sum("savings").as("totalSavings"),
        sum("spend").as("totalSpend"),
      )

    res = AddChannel(res)
    res = AddMarketType(res)
    res.groupBy("PlutusTfModel", "Channel", "SupplyVendor", "DetailedMarketType")
      .agg(
        sum("totalBids").as("totalBids"),
        sum("totalWins").as("totalWins"),
        sum("totalAdjustments").as("totalAdjustments"),
        sum("totalAvailable").as("totalAvailable"),
        sum("totalSavings").as("totalSavings"),
        sum("totalSpend").as("totalSpend")
      ).as[ModelPromotionDetailedStats]
  }
}