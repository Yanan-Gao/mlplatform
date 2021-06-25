package com.thetradedesk.plutus.data.schema

final case class BidRequestRecordV4(GroupId: String,
                                    LogEntryTime: java.sql.Timestamp,
                                    BidRequestId: String,
                                    HandlingDurationInTicks: Long,
                                    AdWidthInPixels: Option[Int],
                                    AdHeightInPixels: Option[Int],
                                    AdjustedBidCPMInUSD: BigDecimal,
                                    RawUrl: Option[String],
                                    TDID: Option[String],
                                    PartnerUserId: Option[String],
                                    SupplyVendor: Option[String],
                                    PartnerRequestData: Option[String],
                                    PartnerResponseData: Option[String],
                                    ReferrerUrl: Option[String],
                                    FPricingSlope: Option[BigDecimal],
                                    FPricingCliff: Option[Int],
                                    Frequency: Long, AdvertiserId: Option[String],
                                    CampaignId: Option[String],
                                    PartnerId: Option[String],
                                    AdGroupId: Option[String],
                                    CreativeId: Option[String],
                                    ReferrerCategories: Seq[String],
                                    MatchedCategory: Option[String],
                                    MatchedSiteFragment: Option[String],
                                    MatchedFoldPosition: Int,
                                    MatchedSiteStrategyLineId: Option[String],
                                    MatchedFoldStrategyLineId: Option[String],
                                    UserHourOfWeek: Option[Int],
                                    AdGroupSelectionAlgorithmConfigHash: Option[String],
                                    Country: Option[String],
                                    Region: Option[String],
                                    Metro: Option[String],
                                    City: Option[String],
                                    Site: Option[String],
                                    DeviceType: Option[DeviceTypeLookupRecord],
                                    OperatingSystemFamily: Option[OSFamilyLookupRecord],
                                    OperatingSystem: Option[OSLookupRecord],
                                    Browser: Option[BrowserLookupRecord],
                                    RenderingContext: Option[RenderingContextLookupRecord],
                                    SupplyVendorPublisherId: Option[String],
                                    RequestLanguages: String, MatchedLanguageCode: Option[String],
                                    VideoQuality: Option[VideoQualityLookupRecord],
                                    IsSecure: Boolean, DealId: Option[String],
                                    PrivateContractId: Option[String],
                                    VideoPlayerSize: Option[VideoPlayerSizeLookupRecord],
                                    SupplyVendorSkippabilityConstraint: Option[SkippabilityConstraintLookupRecord],
                                    DeviceAdvertisingId: Option[String],
                                    SupplyVendorSiteId: Option[String],
                                    BidCPMInCurrency: BigDecimal,
                                    FloorPriceInUSD: Option[BigDecimal],
                                    Carrier: Option[Int],
                                    CampaignFlightId: Option[Long],
                                    PartnerCurrencyExchangeRateFromUSD: BigDecimal,
                                    AdvertiserCurrencyExchangeRateFromUSD: BigDecimal,
                                    DeviceMake: Option[String],
                                    DeviceModel: Option[String],
                                    AvailableImpressionId: Option[String],
                                    AvailableBidRequestId: Option[String],
                                    BidderSharedAssemblyVersion: Option[String],
                                    TestId: Option[String],
                                    RequestPriorityTier: Option[String],
                                    Latitude: Option[Double],
                                    Longitude: Option[Double],
                                    HandlingDurationInMilliseconds: Option[Double],
                                    IPAddress: Option[String],
                                    TemperatureInCelsius: Option[Double],
                                    TemperatureBucketStartInCelsius: Option[Double],
                                    TemperatureBucketEndInCelsius: Option[Double],
                                    Zip: Option[String],
                                    FactualProximityMatchedDesignId: Option[Int],
                                    FactualProximityMatchedTargetingCodeId: Option[Int],
                                    VideoPlaybackType: Option[VideoPlaybackTypeLookupRecord],
                                    BidExpirationTimeInMinutes: Option[Int],
                                    AdapterId: Option[String],
                                    NativePlacementTypeId: Option[Int],
                                    PublisherType: InventoryPublisherTypeLookupRecord,
                                    AuctionType: Option[Int],
                                    ImpressionPlacementId: Option[String],
                                    AdsTxtSellerType: Option[AdsTxtSellerTypeLookupRecord],
                                    RawEventDataStreamEnabled: Option[Boolean],
                                    FirstPriceAdjustment: Option[BigDecimal],
                                    PaymentChainRaw: Option[String],
                                    BidStatus: Option[Int],
                                    PredictionId: Option[String],
                                    AdId: Option[String] = None,
                                    OriginalAvailableImpressionId: Option[String] = None,
                                    StreamingMediaNetwork: Option[String],
                                    StreamingMediaChannel: Option[String],
                                    IsGdprApplicable: Boolean,
                                    GdprConsent: Option[String],
                                    TTDHasConsentForDataSegmenting: Boolean,
                                    VolumeControlPriority: Option[Int],
                                    PartnerHasConsent: Boolean,
                                    BidderCacheMachineName: Option[String] = None,
                                    SupplyVendorDeclaredDealType: Option[Int] = None,
                                    SupplyVendorPublisherName: Option[String] = None,
                                    SupplyVendorPublisherTopLevelDomain: Option[String] = None,
                                    PrivateContractOwningPartnerId: Option[String] = None,
                                    PrivateContractOwningPartnerCurrencyCodeId: Option[String] = None,
                                    PrivateContractOwningPartnerCurrencyExchangeRate: Option[BigDecimal] = None,
                                    ContextualMetadataBlob: Option[String] = None,
                                    ContextualMetadataHash: Option[Long] = None,
                                    ProcessSupplyVendorPublisherFromBidRequest: Option[Boolean] = None,
                                    UserAgent: Option[String] = None, AppStoreUrl: Option[String] = None,
                                    PredictiveClearingMode: Option[PredictiveClearingModeLookupRecord] = None,
                                    PredictiveClearingRandomControl: Boolean = false,
                                    HawkId: Option[String])

case class DeviceTypeLookupRecord(value: Int = 0)

case class OSFamilyLookupRecord(value: Int = 1)

case class OSLookupRecord(value: Int = 101)

case class PredictiveClearingModeLookupRecord(value: Int = 0)

case class BrowserLookupRecord(value: Int = 1)

case class SkippabilityConstraintLookupRecord(value: Int = 0)

case class VideoPlayerSizeLookupRecord(value: Int = 0)

case class VideoQualityLookupRecord(value: Int = 1)

case class RenderingContextLookupRecord(value: Int = 0)

case class VideoPlaybackTypeLookupRecord(value: Int = 0)

case class InventoryPublisherTypeLookupRecord(value: Int = 0)

case class AdsTxtSellerTypeLookupRecord(value: Int = 0)

object PredictiveClearingModeLookupRecord {
  val Disabled = 0
  val AdjustmentNotFound = 1
  val WithFeeUsingOriginalBid = 3
}


object BidRequestDataset {

  val BIDSS3 = f"s3://ttd-datapipe-data/parquet/rtb_bidrequest_cleanfile/v=4/"

}
