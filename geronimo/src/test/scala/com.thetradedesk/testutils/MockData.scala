package com.thetradedesk.testutils

import com.thetradedesk.geronimo.bidsimpression.schema._
import com.thetradedesk.geronimo.shared.schemas._

object MockData {

  // note if you add fields to Impressions will need to add them here too
  def createImpressionsRow(BidRequestId: String = "",
                           PartnerId: String = "",
                           SupplyVendor: String,
                           AdWidthInPixels: Int = 0,
                           AdHeightInPixels: Int = 0,
                           DealId: String = "",
                           DiscrepancyAdjustmentMultiplier: BigDecimal = 0,
                           FirstPriceAdjustment: BigDecimal = 0,
                           MediaCostCPMInUSD: BigDecimal = 0,
                           SubmittedBidAmountInUSD: BigDecimal = 0,
                           BidFeedbackId: String = "",
                           FeedbackBidderCacheMachineName: String = ""
                          ) =
    BidFeedbackRecord(BidRequestId, PartnerId, SupplyVendor, AdWidthInPixels,
      AdHeightInPixels, DealId, DiscrepancyAdjustmentMultiplier,
      FirstPriceAdjustment, MediaCostCPMInUSD, SubmittedBidAmountInUSD,
      BidFeedbackId, FeedbackBidderCacheMachineName)

  // to use request.copy(<manually input fields you want changed>)
  var request = BidRequestRecord(
    GroupId = "202010070754_bidrequest_dc13_jp1-bdl-252cbb-892194570_ci113000_pi1_cs1.log.gz",
    LogEntryTime = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),
    BidRequestId = "10000000-0000-0000-0000-000000000000",
    HandlingDurationInTicks = 10L,
    AdWidthInPixels = Option(1),
    AdHeightInPixels = Option(1),
    AdjustedBidCPMInUSD = 1.0,
    RawUrl = Option("www.espn.com/verstappen-wins-2021-world-champ"),
    TDID = Option("30000000-0000-0000-0000-000000000000"),
    PartnerUserId = Option(""),
    SupplyVendor = Option("nicksSSP"),
    PartnerRequestData = Option(""),
    PartnerResponseData = Option(""),
    ReferrerUrl = Option(""),
    FPricingSlope = Option(10.0),
    FPricingCliff = Option(1),
    Frequency = 12L,
    AdvertiserId = Option("this"),
    CampaignId = Option("that"),
    PartnerId = Option("those"),
    AdGroupId = Option("it"),
    CreativeId = Option("supercoolimage"),
    ReferrerCategories = Seq("cat" , "cats" , "catted"),
    MatchedCategory = Option("yes"),
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
    FloorPriceInUSD = Option(1230.0),
    Carrier = Option(2),
    CampaignFlightId = Option(12L),
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
    ContextualMetadataHash = Option(10l),
    ProcessSupplyVendorPublisherFromBidRequest = Option(true),
    UserAgent = Option(""),
    AppStoreUrl = Option(""),
    PredictiveClearingMode = Option(PredictiveClearingModeLookupRecord()),
    PredictiveClearingRandomControl = false,
    UnifiedId2 = Option("123"),
    HawkId = Option("456"),
    DoNotTrack = Option(DoNotTrackLookupRecord()))

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
    SupplyVendorSiteId = Some(""),
    Site = Some(""),
    ImpressionPlacementId = Some(""),
    AdWidthInPixels = 1,
    AdHeightInPixels = 10,

    MatchedCategory = Some(""),
    MatchedFoldPosition = 1,
    RenderingContext = Option(RenderingContextLookupRecord()),
    ReferrerCategories = Seq(""),

    VolumeControlPriority = Some(1),
    LogEntryTime = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),


    AdsTxtSellerType = Option(AdsTxtSellerTypeLookupRecord()),
    PublisherType = InventoryPublisherTypeLookupRecord(),
    AuctionType = Some(1),



    Country = Some(""),
    Region = Some(""),
    Metro = Some(""),
    City = Some(""),
    Zip = Some(""),


    DeviceType = Option(DeviceTypeLookupRecord()),
    DeviceMake = Some(""),
    DeviceModel = Some(""),
    OperatingSystemFamily = Option(OSFamilyLookupRecord()),
    Browser = Option(BrowserLookupRecord()),

    UserHourOfWeek = Some(0),
    RequestLanguages = "",
    MatchedLanguageCode = Some(""),
    Latitude = Some(0.0d),
    Longitude = Some(0.0d),

    PredictiveClearingMode = Option(PredictiveClearingModeLookupRecord()),
    PredictiveClearingRandomControl = false,

    // bidfeedback cols

    MediaCostCPMInUSD = Some(1.0),
    DiscrepancyAdjustmentMultiplier = Some(1.0),

    SubmittedBidAmountInUSD = 10.0,
    ImpressionsFirstPriceAdjustment = Some(1.0),

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

    PrivateContractId =  ""

  )


}
