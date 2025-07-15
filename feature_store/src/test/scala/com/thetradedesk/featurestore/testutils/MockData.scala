package com.thetradedesk.featurestore.testutils

import com.thetradedesk.featurestore.configs.{DataType, FeatureDefinition, FeatureSourceDefinition, UserFeatureMergeDefinition}
import com.thetradedesk.featurestore.data.cbuffer.CBufferConstants.ArrayLengthKey
import com.thetradedesk.geronimo.bidsimpression.schema._
import com.thetradedesk.geronimo.shared.schemas._
import com.thetradedesk.spark.datasets.generated.SeenInBiddingV2DeviceDataRecord
import com.thetradedesk.spark.datasets.sources.ThirdPartyDataRecord
import com.thetradedesk.streaming.records.rtb.bidfeedback.BidFeedbackRecord
import com.thetradedesk.streaming.records.rtb.bidrequest.BidRequestRecord
import com.thetradedesk.streaming.records.rtb.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, ContextTypeLookupRecord, CrossDeviceVendorIdLookupRecord, CurrencyCodeLookupRecord, DataGroupUsage, DataUsageRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, ElementUsage, ExperimentPgForcedBidTypeLookupRecord, FeeFeatureUsageLogBackingData, IdiosyncraticSegmentLookupRecord, InternetConnectionTypeLookupRecord, InventoryPublisherTypeLookupRecord, MediaTypeLookupRecord, OSFamilyLookupRecord, OSLookupRecord, PredictiveClearingModeLookupRecord, ProductionQualityLookupRecord, RenderingContextLookupRecord, SkippabilityConstraintLookupRecord, VideoPlaybackTypeLookupRecord, VideoPlayerSizeLookupRecord, VideoQualityLookupRecord}
import com.thetradedesk.geronimo.sib.schema.GeronimoSibSchema
import job.ModelEligibleUserDataRecord
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.xxhash64
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time.Instant
import scala.util.Random

object MockData {

  // note if you add fields to Impressions will need to add them here too
  def createImpressionsRow(site: String = "",
                           country: Option[String] = Some(""),
                           region: Option[String] = Some(""),
                           metro: Option[String] = Some(""),
                           city: Option[String] = Some(""),
                           deviceAdvertisingId: Option[String] = Some(""),
                           TDID: String = "",
                           bidFeedbackId: String = "",
                           bidRequestId: String = "",
                           partnerCostInUSD: BigDecimal = BigDecimal.valueOf(1),
                           adWidthInPixels: Int = 0,
                           adHeightInPixels: Int = 0,
                           deviceType: DeviceTypeLookupRecord = DeviceTypeLookupRecord(),
                           supplyVendor: String = "",
                           billingEventId: Option[Long] = Some(1L)
                          ) =
    BidFeedbackRecord(
      GroupId = "",
      LogEntryTime = Timestamp.from(Instant.parse("2020-10-28T00:00:00Z")),
      HandlingDurationInTicks = 0,
      BidFeedbackId = bidFeedbackId,
      AdId = None,
      BidRequestId = bidRequestId,
      MediaCostCPMInUSD = Some(0),
      SupplyVendor = Some(supplyVendor),
      PartnerId = None,
      AdvertiserId = None,
      CampaignId = None,
      AdGroupId = None,
      ChannelId = None,
      CreativeId = None,
      AdWidthInPixels = Some(adWidthInPixels),
      AdHeightInPixels = Some(adHeightInPixels),
      Frequency = 0,
      RawUrl = None,
      IsWinningPriceValid = true,
      FeedbackSentTime = None,
      Site = Some(site),
      TDID = TDID,
      ReferrerCategories = Seq(),
      //MatchedCategoryList = None, // removed: not in el-dorado bidfeedback schema
      MatchedSiteFragment = None,
      MatchedFoldPosition = 0,
      MatchedSiteStrategyLineId = None,
      MatchedFoldStrategyLineId = None,
      UserHourOfWeek = None,
      AdGroupSelectionAlgorithmConfigHash = None,
      Country = country,
      Region = region,
      Metro = metro,
      City = city,
      DiscrepancyAdjustmentMultiplier = None,
      IPAddress = Some(""),
      UserAgent = None,
      DataUsageRecord = None,
      SupplyVendorBillsTTD = false,
      DeviceType = Some(deviceType),
      OperatingSystemFamily = None,
      OperatingSystem = None,
      Browser = None,
      PartnerRequestData = None,
      SupplyVendorPublisherId = None,
      FeeFeaturesCost = 0,
      ReferrerUrl = None,
      RequestLanguageCodes = None,
      MatchedLanguageCode = None,
      FeeFeatureAmounts = Map(),
      DealId = None,
      FeedbackStatus = 0,
      FeedbackAgeInMinutes = 0,
      PrivateContractId = Some("1"),
      TTDCostInUSD = partnerCostInUSD * 0.9,
      PartnerCostInUSD = partnerCostInUSD,
      AdvertiserCostInUSD = partnerCostInUSD * 1.2,
      PartnerCurrencyExchangeRateFromUSD = 1.5,
      AdvertiserCurrencyExchangeRateFromUSD = 1.5,
      CampaignVersion = None,
      FeeSheetVersion = None,
      TDIDCookieStatus = None,
      AdGroupLifetimeFrequency = 0,
      GRPReportingTargetingIds = Seq(),
      GRPGeoSegmentId = None,
      FeeFeaturesApplicableContexts = Map(),
      WinningPriceCPMInCurrency = 0,
      WinningPriceCurrencyCode = None,
      CurrencyExchangeRateCheckpoint = None,
      RenderingContext = None,
      TDIDUsedForBidding = None,
      DeviceAdvertisingId = deviceAdvertisingId,
      Latitude = None,
      Longitude = None,
      TemperatureInCelsius = None,
      TemperatureBucketStartInCelsius = None,
      TemperatureBucketEndInCelsius = None,
      Zip = None,
      OpenStatId = None,
      OpenStatIdVersion = None,
      FactualProximityMatchedDesignId = None,
      FactualProximityMatchedTargetingCodeId = None,
      DeviceMake = None,
      DeviceModel = None,
      TestId = None,
      CarrierId = None,
      NielsenDemoSamplingCampaignId = None,
      NielsenDemoSamplingSiteId = None,
      CampaignFlightId = None,
      TTDMarginInUSD = None,
      FeeFeatureUsage = None,
      ProvisionalFeedback = false,
      VideoPlaybackType = None,
      SupplyVendorSkippabilityConstraint = None,
      DealWinningPriceAdjustment = 0,
      Signature = None,
      SubmittedBidAmountInUSD = 0,
      PartnerCurrency = None,
      AdvertiserCurrency = None,
      AdapterId = None,
      NativePlacementTypeId = None,
      ImpressionSecondsFromEpoch = 0,
      ImpressionPlacementId = None,
      AdsTxtSellerType = None,
      CurrencyBufferCostInUSDollars = None,
      IsManagedByTTD = None,
      FirstPriceAdjustment = None,
      StreamingMediaNetwork = None,
      StreamingMediaChannel = None,
      // Fields from here on are not in the (previously used) Geronimo schema but in the el-dorado bidfeedback schema:
      IsGdprApplicable = false,
      TTDHasConsentForDataSegmenting = true,
      PartnerHasConsent = true,
      VolumeControlPriority = None,
      PredictiveClearingMode = None,
      HouseholdAnchor = None,
      PreventPrivacyIconFee = false,
      PrivateContractOwningPartnerId = None,
      PrivateContractOwningPartnerCurrencyCodeId = None,
      PrivateContractOwningPartnerCurrencyExchangeRate = None,
      ContextualMetadataHash = None,
      SSPAuctionId = None,
      MediaGuardBypassed = false,
      InternetConnectionType = None,
      PredictiveClearingRandomControl = false,
      ImpressionMultiplier = None,
      WhiteOpsPixelSent = false,
      SupplyChainScoringCode = None,
      AuctionType = None,
      ProvisionalFeedbackExpirySeconds = None,
      DealOriginAndType = None,
      ReferrerGenresList = None,
      MatchedGenre = None,
      CustomBidderTrackingId = None,
      ContentDuration = None,
      LiveStream = None,
      IsCcpaApplicable = None,
      UserOptedOutOfDataSale = None,
      BrowserTDID = None,
      TDIDExternal = None,
      DeviceAdvertisingIdExternal = None,
      IPAddressExternal = None,
      LatitudeExternal = None,
      LongitudeExternal = None,
      ZipExternal = None,
      IsMeasurable = None,
      FeedbackBidderCacheMachineName = None,
      WeatherCondition = None,
      HashedIpAsUiid = None,
      InventorySubTypeId = None,
      DomainClassIds = None,
      SubDirectoryLevel1 = None,
      SubDirectoryLevel2 = None,
      AliasedSupplyPublisherId = None,
      DigitalOutOfHomeVenueTypeId = None,
      TTDNativeContextTypeId = None,
      UnifiedId2 = None,
      IdentityLinkId = None,
      HawkId = None,
      IdiosyncraticSegment = None,
      MediaTypeId = None,
      AuctionTypeId = None,
      KoaExperimentStatusId = None,
      ContentEpisode = None,
      ContentTitle = None,
      ContentSeries = None,
      ContentSeason = None,
      ContentProductionQuality = None,
      SSPAuctionIdExternal = None,
      UserAgentExternal = None,
      OperatingSystemExternal = None,
      DeviceModelExternal = None,
      RawUrlExternal = None,
      DataPolicyFlags = None,
      MatchedCategoryList = None,
      MatchedContentRating = None,
      ContentGenre1 = None,
      ContentGenre2 = None,
      ContentGenre3 = None,
      ContentGenre4 = None,
      ContentGenre5 = None,
      QualityScore = None,
      AtmosphericCondition = None,
      DATId = None,
      OtherId = None,
      OtherIdTypeId = None,
      MarketplaceId = None,
      BillingEventId = billingEventId

    )

  // to use request.copy(<manually input fields you want changed>)
  val eu = ElementUsage(123L, Option(""), Option(1.0), Option(""), Option(false), Option(1.0), Option(1.0), Option(1), Option(123L))

  val dgu = DataGroupUsage(Option(""), Option(1), Option(1), Option(1), Option(Seq(eu)), true)

  var request = BidRequestRecord(
    GroupId = "202010070754_bidrequest_dc13_jp1-bdl-252cbb-892194570_ci113000_pi1_cs1.log.gz",
    LogEntryTime = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),
    BidRequestId = "10000000-0000-0000-0000-000000000000",
    HandlingDurationInTicks = 10L,
    AdWidthInPixels = Option(1),
    AdHeightInPixels = Option(1),
    AdjustedBidCPMInUSD = 1.0,
    RawUrl = Option("www.espn.com/verstappen-wins-2021-world-champ"),
    TDID = Option("10000000-0000-0000-0000-000000000000"),
    PartnerUserId = Option(""),
    SupplyVendor = Option("nicksSSP"),
    PartnerRequestData = Option(""),
    PartnerResponseData = Option(""),
    ReferrerUrl = Option("https://www.usnews.com/best-schools/cs"),
    FPricingSlope = Option(10.0),
    FPricingCliff = Option(1),
    Frequency = 12L,
    AdvertiserId = Option("this"),
    CampaignId = Option("that"),
    PartnerId = Option("those"),
    AdGroupId = Option("it"),
    CreativeId = Option("supercoolimage"),
    DataUsageRecord = DataUsageRecord(Option(1.0), Option(""), scala.Seq(dgu), Option(123), Option(""), Option(1), Option(1)),
    ReferrerCategories = Seq("cat", "cats", "catted"),
    MatchedSiteFragment = Option(""),
    MatchedFoldPosition = 1,
    MatchedSiteStrategyLineId = Option(""),
    MatchedFoldStrategyLineId = Option(""),
    UserHourOfWeek = Option(14),
    AdGroupSelectionAlgorithmConfigHash = Option(""),
    Country = Option("United States"),
    Region = Option("Colorado"),
    Metro = Option(""),
    City = Option("Boulder"),
    Site = Option("www.espn.com"),
    DeviceType = Option(DeviceTypeLookupRecord()),
    OperatingSystemFamily = Option(OSFamilyLookupRecord()),
    OperatingSystem = Option(OSLookupRecord()),
    Browser = Option(BrowserLookupRecord()),
    RenderingContext = Option(RenderingContextLookupRecord()),
    SupplyVendorPublisherId = Option(""),
    RequestLanguages = "Japanese",
    MatchedLanguageCode = Option(""),
    VideoQuality = Option(VideoQualityLookupRecord()),
    IsSecure = true,
    DealId = Option(""),
    PrivateContractId = Option(""),
    VideoPlayerSize = Option(VideoPlayerSizeLookupRecord()),
    SupplyVendorSkippabilityConstraint = Option(SkippabilityConstraintLookupRecord()),
    DeviceAdvertisingId = Option("10000000-0000-0000-0000-000000000000"),
    SupplyVendorSiteId = Option(""),
    BidCPMInCurrency = 30.0,
    BidCPMCurrencyCode = CurrencyCodeLookupRecord(),
    CurrencyExchangeRateCheckpoint = Option(java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123")),
    FloorPriceInUSD = Option(1230.0),
    Carrier = Option(2),
    CampaignFlightId = Option(12L),
    DoNotTrack = DoNotTrackLookupRecord(),
    PartnerCurrencyExchangeRateFromUSD = 12.0,
    AdvertiserCurrencyExchangeRateFromUSD = 122.0,
    DeviceMake = Option(""),
    DeviceModel = Option(""),
    AvailableImpressionId = Option("10000000-0000-0000-0000-000000000000"),
    AvailableBidRequestId = Option("10000000-0000-0000-0000-000000000000"),
    BidderSharedAssemblyVersion = Option(""),
    TestId = Option(""),
    RequestPriorityTier = Option(""),
    Latitude = Option(0.0d),
    Longitude = Option(0.0d),
    HandlingDurationInMilliseconds = Option(0.0d),
    IPAddress = Option("63.239.147.18"),
    TemperatureInCelsius = Option(0.0d),
    TemperatureBucketStartInCelsius = Option(0.0d),
    TemperatureBucketEndInCelsius = Option(0.0d),
    Zip = Option("12345"),
    FactualProximityMatchedDesignId = Option(1),
    FactualProximityMatchedTargetingCodeId = Option(1),
    VideoPlaybackType = Option(VideoPlaybackTypeLookupRecord()),
    BidExpirationTimeInMinutes = Option(1),
    AdapterId = Option(""),
    NativePlacementTypeId = Option(1),
    PublisherType = InventoryPublisherTypeLookupRecord(),
    AuctionType = Option(1),
    ImpressionPlacementId = Option(""),
    AdsTxtSellerType = Option(AdsTxtSellerTypeLookupRecord()),
    RawEventDataStreamEnabled = Option(true),
    FirstPriceAdjustment = Option(1.0),
    PaymentChainRaw = Option(""),
    BidStatus = Option(1),
    PredictionId = Option(""),
    AdId = Option(""),
    OriginalAvailableImpressionId = Option("10000000-0000-0000-0000-000000000000"),
    StreamingMediaNetwork = Option(""),
    StreamingMediaChannel = Option(""),
    IsGdprApplicable = false,
    GdprConsent = Option(""),
    TTDHasConsentForDataSegmenting = true,
    VolumeControlPriority = Option(1),
    PartnerHasConsent = true,
    BidderCacheMachineName = Option(""),
    SupplyVendorDeclaredDealType = Option(1),
    SupplyVendorPublisherName = Option(""),
    SupplyVendorPublisherTopLevelDomain = Option(""),
    PrivateContractOwningPartnerId = Option(""),
    PrivateContractOwningPartnerCurrencyCodeId = Option(""),
    PrivateContractOwningPartnerCurrencyExchangeRate = Option(10),
    ContextualMetadataBlob = Option(""),
    ContextualMetadataHash = Option(10L),
    ProcessSupplyVendorPublisherFromBidRequest = Option(true),
    UserAgent = Option(""),
    AppStoreUrl = Option(""),
    MediaGuardBypassed = false,
    InternetConnectionType = Option(InternetConnectionTypeLookupRecord()),
    PredictiveClearingMode = Option(PredictiveClearingModeLookupRecord()),
    PredictiveClearingRandomControl = false,
    SupplyChainDigestHash = Option(""),
    IsCcpaApplicable = Option(false),
    UserOptedOutOfDataSale = Option(false),
    TDIDExternal = Option(""),
    DeviceAdvertisingIdExternal = Option(""),
    IPAddressExternal = Option(""),
    LatitudeExternal = Option(0.0d),
    LongitudeExternal = Option(0.0d),
    ZipExternal = Option(""),
    AliasedSupplyPublisherId = Option(1),
    TTDNativeContextTypeId = Option(1),
    UnifiedId2 = Option("123"),
    IdentityLinkId = Option(""),
    HawkId = Option("456"),
    ConsentString = Option(""),
    ConsentStandard = Option(""),
    IdiosyncraticSegment = Option(IdiosyncraticSegmentLookupRecord()),
    ExperimentId = Option(""),
    RandomizationId = Option(""),
    RandomizationCrossDeviceVendorId = Option(CrossDeviceVendorIdLookupRecord()),
    ExperimentPgForcedBidType = Option(ExperimentPgForcedBidTypeLookupRecord()),
    MediaTypeId = Option(MediaTypeLookupRecord()),
    KoaExperimentStatusId = Option(1),
    ContentEpisode = Option(1),
    ContentTitle = Option(""),
    ContentSeries = Option(""),
    ContentSeason = Option(""),
    ContentProductionQuality = Option(ProductionQualityLookupRecord()),
    ContentContextType = Option(ContextTypeLookupRecord()),
    ContentLengthInSeconds = Option(1),
    ContentGenre = Option(""),
    ContentRating = Option(""),
    SSPAuctionIdExternal = Option(""),
    UserAgentExternal = Option(""),
    OperatingSystemExternal = Option(OSLookupRecord()),
    DeviceModelExternal = Option(""),
    RawUrlExternal = Option(""),
    DataPolicyFlags = Option(10L),
    PerformanceModel = Option(10L),
    MatchedCategoryList = Option(List("yes")),
    PlutusPushdown = Option(0.0d),
    ReferrerCacheUrl = Option("https://www.thetradedesk.com/us"),
    MatchedContentRating = Option(""),
    ContentGenre1 = Option(""),
    ContentGenre2 = Option(""),
    ContentGenre3 = Option(""),
    ContentGenre4 = Option(""),
    ContentGenre5 = Option(""),
    QualityScore = Option(0.0d),
    DATId = Option(""),
    ReferrerContentId = Option(""),
    ReferrerUrlCacheKey = Option(""),
    OtherId = Option(""),
    OtherIdTypeId = Option(1),
    MarketplaceId = Option(""),
    TransactionId = Option(""),
    PlutusTfModel = Option("PlutusMock"),
    UserSegmentCount = Option(1000),
    ExpectedValue = Option(0.001),
    RPacingValue = Option(0.1),
    JanusVariantMap = Option(Map.apply("modelA" -> "versionA1", "modelB" -> "versionB2"))
    //v = Option("1"),
    //date = Option("2020-06-05"),
    //hour = Option("18")
  )

  val bidsImpressionsMock = BidsImpressionsSchema(
    // bidrequest cols
    BidRequestId = "1",
    DealId = "",

    UIID = Option("0000"),

    AdjustedBidCPMInUSD = 10.0,
    BidsFirstPriceAdjustment = Some(1.0),
    FloorPriceInUSD = Some(1.0),

    PartnerId = Some(""),
    AdvertiserId = Some(""),
    CampaignId = Some(""),
    AdGroupId = Some(""),

    SupplyVendor = Some(""),
    SupplyVendorPublisherId = Some(""),
    AliasedSupplyPublisherId = Some(1),
    SupplyVendorSiteId = Some(""),
    Site = Some(""),
    ImpressionPlacementId = Some(""),
    AdWidthInPixels = 1,
    AdHeightInPixels = 10,

    MatchedCategoryList = Some(List("")),
    MatchedFoldPosition = 1,
    RenderingContext = Option(RenderingContextLookupRecord()),
    ReferrerCategories = Seq(""),

    VolumeControlPriority = Some(1),
    LogEntryTime = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),


    AdsTxtSellerType = Option(AdsTxtSellerTypeLookupRecord()),
    PublisherType = InventoryPublisherTypeLookupRecord(),
    AuctionType = Some(1),
    AdvertiserIndustryCategoryId = Some(2L),

    Country = Some(""),
    Region = Some(""),
    Metro = Some(""),
    City = Some(""),
    Zip = Some(""),


    DeviceType = Option(DeviceTypeLookupRecord()),
    DeviceMake = Some(""),
    DeviceModel = Some(""),
    OperatingSystem = Option(OSLookupRecord()),
    OperatingSystemFamily = Option(OSFamilyLookupRecord()),
    Browser = Option(BrowserLookupRecord()),
    InternetConnectionType = Option(InternetConnectionTypeLookupRecord()),

    UserHourOfWeek = Some(0),
    RequestLanguages = "",
    MatchedLanguageCode = Some(""),
    Latitude = Some(0.0d),
    Longitude = Some(0.0d),

    PredictiveClearingMode = Option(PredictiveClearingModeLookupRecord()),
    PredictiveClearingRandomControl = false,
    PlutusTfModel = Option("plutusMock"),

    // bidfeedback cols

    MediaCostCPMInUSD = Some(1.0),
    DiscrepancyAdjustmentMultiplier = Some(1.0),

    SubmittedBidAmountInUSD = 10.0,
    ImpressionsFirstPriceAdjustment = Some(1.0),

    AdvertiserCostInUSD = Some(14),
    PartnerCostInUSD = Some(12.5),
    TTDCostInUSD = Some(12.7),
    AdvertiserCurrencyExchangeRateFromUSD = Some(0.7),

    IsImp = true,

    sin_hour_week = 0.0d,
    cos_hour_week = 0.0d,
    sin_hour_day = 0.0d,
    cos_hour_day = 0.0d,
    sin_minute_hour = 0.0d,
    cos_minute_hour = 0.0d,
    sin_minute_day = 0.0d,
    cos_minute_day = 0.0d,

    DoNotTrack = Option(DoNotTrackLookupRecord()),
    CreativeId = Option(""),

    PrivateContractId = "", //16,777,217
    ReferrerUrl = Some("https://www.usnews.com//best-schools/cs"),
    ContextualCategories = Some(Seq[Long](123456789, 267891234)),
    IsAdFormatOptimizationEnabled = Some(true),
    IsGeoSegmentOptimizationEnabled = Some(false),
    KoaCanBidUpEnabled = Some(false), // assist only with performance
    IsEnabled = Some(true),
    UserSegmentCount = Some(1000),
    MatchedSegments = Seq[Long](20000001, 20000002),
    ExpectedValue = Option(0.001),
    RPacingValue = Option(0.1),
    JanusVariantMap = Option(Map.apply("modelA" -> "versionA1", "modelB" -> "versionB2")),
    ModelVersionsUsed = Option(Map.apply("plutus" -> 111, "kongming" -> 56, "philo" -> 151)),
    BillingEventId = None,
    FeeFeatureUsage = Seq[FeeFeatureUsageLogBackingData](),
    UserAgeInDays = Some(1.5),
    VolumeControlPriorityKeepRate = Some(0.7),

    // SPO holdout field columns
    AdInfoSpoInventoryIdHash = Some(0L),
    AdInfoSpoFilteredStatusId = Some(0),

    VideoPlaybackType = Option(VideoPlaybackTypeLookupRecord()),

    TransactionId = Some(""),
  )

  val advertiserRecordMock = AdvertiserRecord(
    AdvertiserId = "",
    AdvertiserIdInteger = 1,
    PartnerId = "",
    AdvertiserName = "",
    AdvertiserDescription = Some(""),
    CurrencyCodeId = "",
    ContactId = Some(""),
    PaymentMethodTypeId = 1,
    AttributionClickLookbackWindowInSeconds = 1,
    AttributionImpressionLookbackWindowInSeconds = 1,
    ClickDedupeWindowInSeconds = 1,
    ConversionDeDupeWindowInSeconds = 1,
    RightMediaDefaultOfferTypeId = Some(1L),
    RecencyBucketListId = "",
    CreatedAt = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),
    LastUpdatedAt = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),
    IsVisible = true,
    AdvertiserFacebookAccountId = Some(1L),
    FacebookAdAccountId = Some(1L),
    ManagerId = Some(""),
    BuyerId = Some(""),
    MaximumNumberOfIpRanges = 1,
    MaximumNumberOfSiteListLines = 1,
    IndustryCategoryId = Some(1L),
    ExcludeFromReporting = false,
    PrimaryAttributionCluster = Some(1),
    DailyConversionLimit = Some(1L),
    SecondaryAttributionCluster = Some(1),
    AdvertiserRevenueGroupId = "",
    HasLogo = false,
    MaximumNumberOfTemperatureAdjustmentsPerAdGroup = 1,
    DefaultAdGroupTemperatureBucketListId = 1,
    ReportingTemperatureBucketListId = 1,
    WitholdTTDMarginFromAdvertiserCostOverride = Some(false),
    IncludeTTDMarginInAdvertiserCost = false,
    BaiduIndustryCategoryId = Some(1L),
    InvoiceTimeZoneId = Some(1),
    UseMediaCostBasisForCampaignFees = false,
    OverridePartnerDefaultImpressionTrackingUrl = false,
    OverridePartnerDefaultImpressionTrackingUrl2 = false,
    OverridePartnerDefaultImpressionTrackingUrl3 = false,
    DefaultThirdPartyImpressionTrackingUrl = Some(""),
    DefaultThirdPartyImpressionTrackingUrl2 = Some(""),
    DefaultThirdPartyImpressionTrackingUrl3 = Some(""),
    AdsTxtSiteUnauthorizedHandlingOverrideId = 1,
    AdvertiserLegalName = Some(""),
    AdvertiserBrandName = Some(""),
    IsGdprWhitelisted = true,
    DontUsePreGdprFirstPartyData = Some(false),
    AdvertiserCorporationId = Some("")
  )

  val contextual = ContextualCategoryRecord(
    UrlHash = "c530646f7a1a7ce3432525c89cd87768",
    ReferrerCacheUrl = "https://www.thetradedesk.com/us",
    StateId = 1,
    NoContent = false,
    RequestedDateTimeUtc = java.sql.Timestamp.valueOf("2023-04-27 16:30:00"),
    StandardCategoryIds = Seq[Long](123, 456, 78))

  var sibMock: SeenInBiddingV2DeviceDataRecord = SeenInBiddingV2DeviceDataRecord(
    Tdid = "10000000-0000-0000-0000-000000000000",
    FirstPartyTargetingDataIds = Seq[Long](1000001, 2000001, 3000001),
    ThirdPartyTargetingDataIds = Seq[Long](1000003, 2000003, 3000003, 4000003)
  )

  var sibMockNoData: SeenInBiddingV2DeviceDataRecord = SeenInBiddingV2DeviceDataRecord(
    Tdid = "10000000-0000-0000-0000-000000000000",
    FirstPartyTargetingDataIds = Seq[Long](),
    ThirdPartyTargetingDataIds = Seq[Long]()
  )

  var sibGroupMock: SeenInBiddingGroupRecord = SeenInBiddingGroupRecord(
    GroupData = "X0000000-0000-0000-0000-000000000000",
    Tdids = Seq("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
    FirstPartyTargetingDataIds = Seq[Long](1000001, 2000001, 3000001),
    ThirdPartyTargetingDataIds = Seq[Long](1000003, 2000003, 3000003, 4000003, 1000004)
  )

  var sibGroup2Mock: SeenInBiddingGroupRecord = SeenInBiddingGroupRecord(
    GroupData = "X0000000-0000-0000-0000-000000000001",
    Tdids = Seq("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
    FirstPartyTargetingDataIds = Seq[Long](1000001, 2000001, 3000001),
    ThirdPartyTargetingDataIds = Seq[Long](1000003, 2000004, 1000005)
  )

  var sibGroupMockNoData: SeenInBiddingGroupRecord = SeenInBiddingGroupRecord(
    GroupData = "X0000000-0000-0000-0000-000000000000",
    Tdids = Seq("10000000-0000-0000-0000-000000000000", "20000000-0000-0000-0000-000000000000"),
    FirstPartyTargetingDataIds = Seq[Long](1000001, 2000001, 3000001),
    ThirdPartyTargetingDataIds = Seq[Long]()
  )

  // the next two records would be the output of sibMock + sibGroupMock
  var geronimoSibMock1: GeronimoSibSchema = GeronimoSibSchema(
    UIIDHash = BigInt(-2080483288897205235L), // xxhash64 of the TDID
    TDID = "10000000-0000-0000-0000-000000000000",
    ThirdPartyTargetingDataIds = Seq[Long](1000003),
    GroupThirdPartyTargetingDataIds = Seq[Long](1000003, 1000004)
  )

  var geronimoSibMock2: GeronimoSibSchema = GeronimoSibSchema(
    UIIDHash = BigInt(-8695409125913502992L), // xxhash64 of the TDID
    TDID = "20000000-0000-0000-0000-000000000000",
    ThirdPartyTargetingDataIds = Seq[Long](),
    GroupThirdPartyTargetingDataIds = Seq[Long](1000003, 1000004)
  )

  // third example for Geronimo SIB data with TDID not present in the bid request data
  var geronimoSibMock3: GeronimoSibSchema = GeronimoSibSchema(
    UIIDHash = BigInt(6530277613528693211L), // xxhash64 of the TDID
    TDID = "12345678-0000-0000-0000-000000000000",
    ThirdPartyTargetingDataIds = Seq[Long](1000003, 1000004, 1000005),
    GroupThirdPartyTargetingDataIds = Seq[Long](1000003, 1000004, 1000005)
  )

  var matchedUserdataMock: ModelEligibleUserDataRecord = ModelEligibleUserDataRecord(
    BidRequestId = "brq-id",
    MatchedSegments = Seq[Long](2000003, 2000004, 2000005)
  )


  var koaSettingsMock: AdGroupKoaOptimizationSettingsRecord = AdGroupKoaOptimizationSettingsRecord(
    AdGroupId = Some("it"),
    IsAdFormatOptimizationEnabled = Some(true),
    IsGeoSegmentOptimizationEnabled = Some(false),
    KoaCanBidUpEnabled = Some(false), // assist only with performance
    IsEnabled = Some(true)
  )

  var tpdMockNoData: ThirdPartyDataRecord = ThirdPartyDataRecord(
    Date = 1,
    ThirdPartyDataId = 2,
    ThirdPartyDataProviderId = "thetradedesk",
    ThirdPartyDataProviderElementId = "a",
    ThirdPartyDataParentId = None,
    TargetingDataId = 1000003,
    Name = "b",
    Description = None,
    ThirdPartyDataHierarchyString = "a",
    FullPath = "A",
    Buyable = true,
    ImportEnabled = true,
    AudienceSize = None,
    GeneralPopulationOverlapLastUpdated = None,
    UpdateLive = true,
    AllowCustomFullPath = true,
    DefaultSortScore = 1.0f)

  var tpdMockNoData2: ThirdPartyDataRecord = ThirdPartyDataRecord(
    Date = 1,
    ThirdPartyDataId = 2,
    ThirdPartyDataProviderId = "thetradedesk",
    ThirdPartyDataProviderElementId = "a",
    ThirdPartyDataParentId = None,
    TargetingDataId = 1000004,
    Name = "b",
    Description = None,
    ThirdPartyDataHierarchyString = "a",
    FullPath = "A",
    Buyable = true,
    ImportEnabled = true,
    AudienceSize = None,
    GeneralPopulationOverlapLastUpdated = None,
    UpdateLive = true,
    AllowCustomFullPath = true,
    DefaultSortScore = 1.0f)

  var tpdMockNoData3: ThirdPartyDataRecord = ThirdPartyDataRecord(
    Date = 1,
    ThirdPartyDataId = 2,
    ThirdPartyDataProviderId = "thetradedesk",
    ThirdPartyDataProviderElementId = "a",
    ThirdPartyDataParentId = None,
    TargetingDataId = 1000005,
    Name = "b",
    Description = None,
    ThirdPartyDataHierarchyString = "a",
    FullPath = "A",
    Buyable = true,
    ImportEnabled = true,
    AudienceSize = None,
    GeneralPopulationOverlapLastUpdated = None,
    UpdateLive = true,
    AllowCustomFullPath = true,
    DefaultSortScore = 1.0f)

  var featureLogDataMock: GeronimoCommonFeatureInBidderCache = GeronimoCommonFeatureInBidderCache(
    BidRequestId = "1",
    UserAgeInDays = 7,
    FCapCounterArray = Array[FCapCounter](),
    AdInfoSpoInventoryIdHash = 0l,
    AdInfoSpoFilteredStatusId = 0
  )


  case class TestInputDataSchema(FeatureKey: String,
                                 CategoricalFeat1: Option[String],
                                 ArrayCategoricalFeat1: Array[String],
                                 IntFeat1: Option[Int],
                                 FloatFeat1: Float,
                                 DoubleFeat1: Double)

  val inputDfMock1: TestInputDataSchema = TestInputDataSchema(
    FeatureKey = "a",
    CategoricalFeat1 = Some("google.com"),
    ArrayCategoricalFeat1 = Array("111", "222", "333"),
    IntFeat1 = Some(1.toInt),
    FloatFeat1 = 1.5.toFloat,
    DoubleFeat1 = 3.toDouble
  )

  val inputDfMock2: TestInputDataSchema = TestInputDataSchema(
    FeatureKey = "a",
    CategoricalFeat1 = Some("yahoo.com"),
    ArrayCategoricalFeat1 = Array("111", "222"),
    IntFeat1 = Some(9.toInt),
    FloatFeat1 = 7.5.toFloat,
    DoubleFeat1 = 1.2.toDouble
  )

  val inputDfMock3: TestInputDataSchema = TestInputDataSchema(
    FeatureKey = "a",
    CategoricalFeat1 = Some("foxmail.com"),
    ArrayCategoricalFeat1 = Array("111", "999"),
    IntFeat1 = None,
    FloatFeat1 = 2.5.toFloat,
    DoubleFeat1 = 0.001.toDouble
  )

  val inputDfMock4: TestInputDataSchema = TestInputDataSchema(
    FeatureKey = "b",
    CategoricalFeat1 = Some("foxmail.com"),
    ArrayCategoricalFeat1 = Array("123", "999"),
    IntFeat1 = Some(2.toInt),
    FloatFeat1 = 2.5.toFloat,
    DoubleFeat1 = 0.001.toDouble
  )

  val inputDfMock5: TestInputDataSchema = TestInputDataSchema(
    FeatureKey = "b",
    CategoricalFeat1 = None,
    ArrayCategoricalFeat1 = Array("111", "999"),
    IntFeat1 = Some(5.toInt),
    FloatFeat1 = 0.toFloat,
    DoubleFeat1 = 0.1.toDouble
  )

  def featureSourceSchema(userFeatureMergeDefinition: UserFeatureMergeDefinition, enableBinary: Boolean = true, cbuffer: Boolean = false) = StructType(
    (userFeatureMergeDefinition
      .featureSourceDefinitions
      .flatMap(e => e.features.map(featureDefinitionToStructField(e, _, enableBinary, cbuffer)))
      :+ StructField("TDID", StringType, false))
  )

  private def featureDefinitionToStructField(featureSourceDefinition: FeatureSourceDefinition, featureDefinition: FeatureDefinition, enableBinary: Boolean, cbuffer: Boolean): StructField = {
    if (featureDefinition.arrayLength == 0) {
      StructField(s"${featureSourceDefinition.name}_${featureDefinition.name}", dataTypeToSparkType(featureDefinition.dtype), nullable = false)
    } else {
      val metadata = new MetadataBuilder().putLong(ArrayLengthKey, if (featureDefinition.arrayLength == -1 || (!cbuffer && featureDefinition.arrayLength == 1)) 0 else featureDefinition.arrayLength).build()
      if (featureDefinition.dtype == DataType.Byte && enableBinary) {
        StructField(s"${featureSourceDefinition.name}_${featureDefinition.name}", BinaryType, nullable = false, metadata = metadata)
      } else {
        StructField(s"${featureSourceDefinition.name}_${featureDefinition.name}", ArrayType(dataTypeToSparkType(featureDefinition.dtype), containsNull = false), nullable = false, metadata = metadata)
      }
    }
  }

  private def dataTypeToSparkType(dataType: DataType) = {
    dataType match {
      case DataType.Bool => BooleanType
      case DataType.Byte => ByteType
      case DataType.Short => ShortType
      case DataType.Int => IntegerType
      case DataType.Long => LongType
      case DataType.Float => FloatType
      case DataType.Double => DoubleType
      case DataType.String => StringType
      case _ => throw new RuntimeException("should not happen")
    }
  }

  var inc = 0
  var rnd = new Random()

  def randomFeatureSourceDataMock(userFeatureMergeDefinition: UserFeatureMergeDefinition, cbuffer: Boolean = false): Row = {
    inc += 1

    Row(userFeatureMergeDefinition
      .featureSourceDefinitions
      .flatMap(e => e.features.map(featureDefinitionToStructField(_, cbuffer)))
      :+ inc.toString: _*)
  }

  private def featureDefinitionToStructField(featureDefinition: FeatureDefinition, cbuffer: Boolean) = {
    if (featureDefinition.arrayLength == 0) {
      dataTypeToValue(featureDefinition.dtype)
    } else if (featureDefinition.arrayLength == -1 || (!cbuffer && featureDefinition.arrayLength == 1)) {
      repeatDataType(featureDefinition.dtype, rnd.nextInt(4))
    } else {
      repeatDataType(featureDefinition.dtype, featureDefinition.arrayLength)
    }
  }

  private def repeatDataType(dataType: DataType, n: Int) = {
    dataType match {
      case DataType.Bool => (1 to n).map(_ => rnd.nextBoolean()).toArray
      case DataType.Byte => (1 to n).map(_ => rnd.nextInt(127).asInstanceOf[Byte]).toArray
      case DataType.Short => (1 to n).map(_ => rnd.nextInt(1024).asInstanceOf[Short]).toArray
      case DataType.Int => (1 to n).map(_ => rnd.nextInt()).toArray
      case DataType.Long => (1 to n).map(_ => rnd.nextLong()).toArray
      case DataType.Float => (1 to n).map(_ => rnd.nextFloat()).toArray
      case DataType.Double => (1 to n).map(_ => rnd.nextDouble()).toArray
      case _ => throw new RuntimeException("should not happen")
    }
  }

  private def dataTypeToValue(dataType: DataType) = {
    dataType match {
      case DataType.Bool => rnd.nextBoolean()
      case DataType.Byte => rnd.nextInt(127).asInstanceOf[Byte]
      case DataType.Short => rnd.nextInt(1024).asInstanceOf[Short]
      case DataType.Int => rnd.nextInt().asInstanceOf[Int]
      case DataType.Long => rnd.nextLong()
      case DataType.Float => rnd.nextFloat().asInstanceOf[Float]
      case DataType.Double => rnd.nextDouble()
      case DataType.String => rnd.nextInt().toString
      case _ => throw new RuntimeException("should not happen")
    }
  }
}
