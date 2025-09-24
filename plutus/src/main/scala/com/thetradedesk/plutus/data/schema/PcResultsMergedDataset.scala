package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.utils.S3HourlyParquetDataset
import com.thetradedesk.plutus.data.{paddedDatePart, paddedDateTimePart}
import com.thetradedesk.spark.datasets.core.S3Roots

import java.time.{LocalDate, LocalDateTime}

case class PcResultsMergedSchema(
                                   // bidrequest cols
                                   BidRequestId: String,
                                   DealId: String,

                                   UIID: Option[String],
                                   IdType: Option[String],
                                   IdCount: Int,

                                   PropertyIdString: Option[String],

                                   AdjustedBidCPMInUSD: Double,
                                   BidsFirstPriceAdjustment: Option[Double],
                                   FloorPriceInUSD: Option[Double],

                                   PartnerId: Option[String],
                                   AdvertiserId: Option[String],
                                   CampaignId: Option[String],
                                   AdGroupId: Option[String],

                                   SupplyVendor: Option[String],
                                   SupplyVendorPublisherId: Option[String],
                                   AliasedSupplyPublisherId: Option[Int],
                                   SupplyVendorSiteId: Option[String],
                                   Site: Option[String],
                                   ImpressionPlacementId: Option[String],
                                   AdWidthInPixels: Int,
                                   AdHeightInPixels: Int,

                                   MatchedCategoryList: Option[List[String]],
                                   MatchedFoldPosition: Int,
                                   RenderingContext: Int,
                                   ReferrerCategories: Seq[String],

                                   VolumeControlPriority: Option[Int],
                                   VolumeControlPriorityKeepRate: Option[Double],
                                   LogEntryTime: java.sql.Timestamp,

                                   AdsTxtSellerType: Int,
                                   PublisherType: Int,
                                   AuctionType: Option[Int],

                                   Country: Option[String],
                                   Region: Option[String],
                                   Metro: Option[String],
                                   City: Option[String],
                                   Zip: Option[String],

                                   DeviceType: Int,
                                   DeviceMake: Option[String],
                                   DeviceModel: Option[String],
                                   OperatingSystem: Int,
                                   OperatingSystemFamily: Int,
                                   Browser: Int,
                                   InternetConnectionType: Int,

                                   UserHourOfWeek: Option[Int],
                                   RequestLanguages: String,
                                   MatchedLanguageCode: Option[String],
                                   Latitude: Option[Double],
                                   Longitude: Option[Double],

                                   PredictiveClearingRandomControl: Boolean = false,

                                   LiveStream: Option[Boolean],

                                   // bidfeedback cols
                                   MediaCostCPMInUSD: Option[Double],
                                   DiscrepancyAdjustmentMultiplier: Option[Double],
                                   AdvertiserCostInUSD: Option[Double],
                                   PartnerCostInUSD: Option[Double],
                                   TTDCostInUSD: Option[Double],
                                   AdvertiserCurrencyExchangeRateFromUSD: Option[Double],

                                   SubmittedBidAmountInUSD: Double,
                                   ImpressionsFirstPriceAdjustment: Option[Double],

                                   // computed columns
                                   IsImp: Boolean,

                                   sin_hour_week: Double,
                                   cos_hour_week: Double,
                                   sin_hour_day: Double,
                                   cos_hour_day: Double,
                                   sin_minute_hour: Double,
                                   cos_minute_hour: Double,
                                   sin_minute_day: Double,
                                   cos_minute_day: Double,

                                   DoNotTrack: Int,
                                   CreativeId: Option[String],

                                   PrivateContractId: String,

                                   FeeAmount: Option[Double], // Fee we charge our users for PC

                                   MediaTypeId: Int, // from AdFormatDataSet
                                   Channel: String,
                                   ChannelSimple: String,

                                   DetailedMarketType: String, // from PrivateContractRecord

                                   JanusVariantMap: Option[Map[String, String]],
                                   IsUsingJanus: Boolean,

                                   PlutusVersionUsed: Option[Long], // values are MLFlow Versions, parsed from ModelVersionsUsed

                                   // Coalesced AliasedSupplyPublisherId and SupplyVendorPublisherId
                                   AspSvpId: String,

                                   // Fields from PCResults Log Dataset
                                   InitialBid: Double,
                                   FinalBidPrice: Double,
                                   Discrepancy: Double,
                                   BaseBidAutoOpt: Double,
                                   OptOutDueToFloor: Boolean,
                                   FloorPrice: Double,
                                   PartnerSample: Boolean,
                                   BidBelowFloorExceptedSource: Int, // 0 = off 1 = gauntlet bbf 2 = plutus bbf 
                                   FullPush: Boolean,

                                   // Fields From PlutusLog
                                   Mu: Float,
                                   Sigma: Float,
                                   GSS: Double,
                                   AlternativeStrategyPush: Double,

                                   // Fields from PredictiveClearingStrategy
                                   Model: String,
                                   Strategy: Int, // Contains either the pushdown reducer, open path adjustment, or campaign pc adjustment value applied

                                   LossReason: Int, // from MinimumBidToWin data
                                   WinCPM: Double, // from MinimumBidToWin data
                                   mbtw: Double, // from MinimumBidToWin data

                                   isMbtwValidStrict: Boolean,
                                   isMbtwValid: Boolean,

                                   // User Data Fields

                                   UserSegmentCount: Option[Int],
                                   UserAgeInDays: Option[Double],
                                   MatchedSegments: Seq[Long],

                                   // Value Pacing Fields

                                   ExpectedValue: Option[Double],
                                   RPacingValue: Option[Double],

                                   IsValuePacing: Option[Boolean], // from ProductionAdgroupBudgetData
                                   IsUsingPIDController: Option[Boolean], // from ProductionAdgroupBudgetData

                                   // maxbid cap on bids before plutus 
                                   UseUncappedBidForPushdown:Boolean,
                                   UncappedFirstPriceAdjustment:Double,
                                   UncappedBidPrice: Double,
                                   SnapbackMaxBid: Double,
                                   MaxBidMultiplierCap: Double,

                                   FloorBuffer: Double,
                                   FloorBufferAdjustment: Double,
                                   MaxBidCpmInBucks: Double,

                                   VideoPlaybackTypeValue: Option[Int],

                                   AdRefresh: Option[Int],
                                   StreamingMediaNetwork: Option[String],
                                   StreamingMediaChannel: Option[String],
                                   ContentLengthInSeconds: Option[Int],
                                   ContentProductionQualityValue: Option[Int],
                                   MatchedContentRating: Option[String],

                                   ContentGenre1: Option[String],
                                   ContentEpisode: Option[Int],
                                   ContentTitle: Option[String],
                                   ContentSeries: Option[String],
                                   ContentSeason: Option[String],
                                   ContentContextTypeValue: Option[Int],

                                   Carrier: Option[Int],
                                   DisplayViewabilityScore: Option[Double],
                                   VideoViewabilityScore: Option[Double],
                                   VideoPlayerSizeValue: Option[Int],

                                   IdiosyncraticSegmentValue: Option[Int],
                                   InventoryChannelValue: Option[Int],

                                   NativePlacementTypeId: Option[Int],
                                   QualityScore: Option[Double],
                                   SupplyVendorSkippabilityConstraintValue: Option[Int],
                                   TTDNativeContextTypeId: Option[Int],

                                   TemperatureInCelsius: Option[Double],

                                   SyntheticTransactionId: Option[String],
                                   DealFloorMultiplierCap: Double,
                                   ImpressionMultiplier: Option[Double],

                                   CurrencyCodeId: Option[String],

                                   PredictiveClearingEnabled: Boolean,
                                   PredictiveClearingMode: Int,
                                   TtdLibraryFloorPrice: Option[Double]

                                   // Note: I've removed a bunch of fields from the Geronimo schema since
                                   // these fields dont seem relevant to plutus.

                                   // Deprecated Fields:

                                   // Not being used anymore
                                   // LegacyPcPushdown: Double,

                                   // The loss reasons in this are not the same as the ones in the loss reasons table
                                   // SupplyVendorLossReason: Int,

                                   // Deprecating plutusTfModel. We should be using the Model field instead.
                                   // PlutusTfModel: Option[String],
                                 )

object PcResultsMergedDataset extends S3HourlyParquetDataset[PcResultsMergedSchema]  {
  val DATA_VERSION = 3

  override protected def genHourSuffix(datetime: LocalDateTime): String =
    f"hour=${datetime.getHour}"

  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String =
    f"${S3Roots.IDENTITY_ROOT}/${env}/pcresultsgeronimo/v=${DATA_VERSION}"

  /** DEPRECATED */
  @deprecated val DEFAULT_TTD_ENV = "prod"

  @deprecated val S3_PATH: Option[String] => String = (ttdEnv: Option[String]) => f"${S3Roots.IDENTITY_ROOT}/${ttdEnv.getOrElse(DEFAULT_TTD_ENV)}/pcresultsgeronimo/v=${DATA_VERSION}"

  @deprecated val S3_PATH_HOUR: (LocalDateTime, Option[String]) => String = (dateTime: LocalDateTime, ttdEnv: Option[String]) => f"${S3_PATH(ttdEnv)}/${paddedDateTimePart(dateTime)}"
  @deprecated val S3_PATH_DATE: (LocalDate, Option[String]) => String = (date: LocalDate, ttdEnv: Option[String]) => f"${S3_PATH(ttdEnv)}/date=${paddedDatePart(date)}"

  @deprecated def S3_PATH_DATE_GEN = (date: LocalDate) => {
    f"/date=${paddedDatePart(date)}"
  }

  @deprecated def S3_PATH_HOUR_GEN = (dateTime: LocalDateTime) => {
    f"/date=${paddedDatePart(dateTime.toLocalDate)}/hour=${dateTime.getHour}%02d"
  }


}
