package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.paddedDateTimePart
import com.thetradedesk.spark.datasets.core.S3Roots
import job.PcResultsGeronimoJob.ttdEnv

import java.time.LocalDateTime

case class PcResultsMergedSchema(
                                  // bidrequest cols
                                  BidRequestId: String,
                                  DealId: String,

                                  UIID: Option[String],

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
                                  RequestLanguages: String, MatchedLanguageCode: Option[String],
                                  Latitude: Option[Double],
                                  Longitude: Option[Double],

                                  PredictiveClearingMode: Int,
                                  PredictiveClearingRandomControl: Boolean = false,

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

                                  DetailedMarketType: String, // from PrivateContractRecord

                                  IsUsingJanus: Boolean,

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
                                  BidBelowFloorExceptedSource: Int,
                                  FullPush: Boolean,

                                  // Fields From PlutusLog
                                  Mu: Float,
                                  Sigma: Float,
                                  GSS: Double,
                                  AlternativeStrategyPush: Double,

                                  // Fields from PredictiveClearingStrategy
                                  Model: String,
                                  Strategy: Int, // Contains the pushdown reducer value

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

object PcResultsMergedDataset {
  val S3PATH = f"${S3Roots.IDENTITY_ROOT}/${ttdEnv}/pcresultsgeronimo/v=3/"

  def S3PATH_GEN = (dateTime: LocalDateTime) => paddedDateTimePart(dateTime)
}
