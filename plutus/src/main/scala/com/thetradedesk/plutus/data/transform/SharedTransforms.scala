package com.thetradedesk.plutus.data.transform

import com.thetradedesk.plutus.data.{AuctionType, ChannelType, DeviceTypeId, MarketType, MediaTypeId, PublisherRelationshipType, RenderingContext}
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord, PrivateContractRecord}
import org.apache.spark.sql.{DataFrame, Dataset}
import job.ModelPromotion.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{broadcast, col, lit, when}

object SharedTransforms {
  def AddMediaTypeFromAdFormat(df: DataFrame, af: Dataset[AdFormatRecord]): DataFrame = {
    df.alias("df")
      .join(broadcast(af).alias("af"), $"df.AdFormat" === $"af.DisplayNameShort", "left")
      .select($"df.*", $"MediaTypeId")
  }

  // Define the function get Channel from Ids
  def AddChannel(df: DataFrame, mediaTypeCol: String = "MediaTypeId", renderingContextCol: String = "RenderingContext.value", deviceTypeCol: String = "DeviceType.value"): DataFrame = {
    df
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
      .withColumn("ChannelSimple",
        when($"Channel".isin(ChannelType.MobileInApp, ChannelType.MobileOptimizedWeb, ChannelType.MobileStandardWeb), ChannelType.Display)
          .when($"Channel".isin(ChannelType.MobileVideoInApp, ChannelType.MobileVideoOptimizedWeb, ChannelType.MobileVideoStandard), ChannelType.Video)
          .when($"Channel".isin(ChannelType.NativeVideo, ChannelType.Native), ChannelType.Native)
          .otherwise($"Channel"))
  }

  // If dataset only has AdFormat, you need to get the MediaTypeId from AdFormat before applying AddChannel
  def AddChannelUsingAdFormat(df: DataFrame, adFormatData: Dataset[AdFormatRecord], mediaTypeCol: String = "MediaTypeId", renderingContextCol: String = "RenderingContext.value", deviceTypeCol: String = "DeviceType.value"): DataFrame = {
    val getMediaTypeId = AddMediaTypeFromAdFormat(df, adFormatData)
    AddChannel(getMediaTypeId, mediaTypeCol, renderingContextCol, deviceTypeCol)
  }

  def AddMarketType(df: DataFrame, privateContractsData: Dataset[PrivateContractRecord]): DataFrame = {
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
      .select($"df.*", $"DetailedMarketType")
  }

  def AddDeviceTypeIdAndRenderingContextId(df: DataFrame, renderingContextCol: String = "RenderingContext.value", deviceTypeCol: String = "DeviceType.value"): DataFrame = {
    df
      .withColumn("RenderingContextId",
        when(col(renderingContextCol) === "Other", RenderingContext.Other)
          .when(col(renderingContextCol) === "InApp", RenderingContext.InApp)
          .when(col(renderingContextCol) === "MobileOptimizedWeb", RenderingContext.MobileOptimizedWeb)
      )
      .withColumn("DeviceTypeId",
        when(col(deviceTypeCol) === "Other", DeviceTypeId.Other)
          .when(col(deviceTypeCol) === "PC", DeviceTypeId.PC)
          .when(col(deviceTypeCol) === "Tablet", DeviceTypeId.Tablet)
          .when(col(deviceTypeCol) === "Mobile", DeviceTypeId.Mobile)
          .when(col(deviceTypeCol) === "Roku", DeviceTypeId.Roku)
          .when(col(deviceTypeCol) === "ConnectedTV", DeviceTypeId.ConnectedTV)
          .when(col(deviceTypeCol) === "OutOfHome", DeviceTypeId.OutOfHome)
      )
  }

}
