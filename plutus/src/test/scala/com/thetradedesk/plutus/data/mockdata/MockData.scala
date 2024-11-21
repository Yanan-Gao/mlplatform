package com.thetradedesk.plutus.data.mockdata

import com.thetradedesk.geronimo.bidsimpression.schema._
import com.thetradedesk.plutus.data.MediaTypeId
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdFormatRecord, AdGroupRecord, CampaignRecord, CountryRecord, PrivateContractRecord, ROIGoalTypeRecord, SupplyVendorRecord}
import com.thetradedesk.streaming.records.rtb._
import org.apache.spark.sql.Dataset

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

object MockData {

  val supplyVendorBidding = Svb(RequestName= "Google", SupplyVendorId = "1", DiscrepancyAdjustment = 0.1)
  val partnerSupplyVendorDiscrepancyAdj = Pda(SupplyVendorName = "Google", PartnerId = "1", DiscrepancyAdjustment = 0.2)
  val supplyVendorDealRecord = Deals(SupplyVendorId="1", SupplyVendorDealCode = "", IsVariablePrice = true)


  def createMbToWinRow(
                        LogEntryTime: String = "2021-12-25",
                        BidRequestId: String,
                        CreativeId: String = "",
                        AdGroupId: String = "",
                        CampaignId: String = "",
                        PrivateContractId: String = "",
                        PartnerId: String = "",
                        AdvertiserId: String = "",
                        CampaignFlightId: String = "",
                        SupplyVendorLossReason: Integer = 1,
                        LossReason: Integer = 1,
                        WinCPM: Double = 1.0d,
                        SupplyVendor: String = "",
                        BidRequestTime: String = "",
                        mbtw: Double = 1.0d                      ) =
    RawLostBidData(LogEntryTime, BidRequestId, CreativeId, AdGroupId, CampaignId, PrivateContractId, PartnerId, AdvertiserId, CampaignFlightId, SupplyVendorLossReason, LossReason, WinCPM, SupplyVendor, BidRequestTime, mbtw)


  def bidsImpressionsMock(FeeFeatureUsage: Seq[FeeFeatureUsageLogBackingData] = Seq(),
                          JanusVariantMap: Option[Map[String, String]] = None,
                          ModelVersionsUsed: Option[Map[String, Long]] = None
                         ) =
    BidsImpressionsSchema(
      // bidrequest cols
      BidRequestId = "1",
      DealId = "",

      UIID = Option("000"),

      AdjustedBidCPMInUSD = 10000.0,
      BidsFirstPriceAdjustment = Some(0.9),
      FloorPriceInUSD = Some(5000.0),

      PartnerId = Some(""),
      AdvertiserId = Some(""),
      CampaignId = Some(""),
      AdGroupId = Some("fakeadgroup123"),

      SupplyVendor = Some(""),
      SupplyVendorPublisherId = Some("983"),
      AliasedSupplyPublisherId = Some(1),
      SupplyVendorSiteId = Some(""),
      Site = Some(""),
      ImpressionPlacementId = Some(""),
      AdWidthInPixels = 250,
      AdHeightInPixels = 250,

      MatchedCategoryList =  Some(List("")),
      MatchedFoldPosition = 1,
      RenderingContext = Option(RenderingContextLookupRecord(1)),
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
      PlutusTfModel = Some("plutusMock"),
      VolumeControlPriorityKeepRate = None,

      // bidfeedback cols

      MediaCostCPMInUSD = Some(9000.0),
      DiscrepancyAdjustmentMultiplier = Some(1.0),
      AdvertiserCostInUSD = None,
      PartnerCostInUSD = None,
      TTDCostInUSD = None,
      AdvertiserCurrencyExchangeRateFromUSD = None,

      SubmittedBidAmountInUSD = 10.0,
      ImpressionsFirstPriceAdjustment = Some(0.9),

      BillingEventId = None,
      FeeFeatureUsage= FeeFeatureUsage,

      IsImp = true,

      sin_hour_week = 0.0d,
      cos_hour_week = 0.0d,
      sin_hour_day = 0.0d,
      cos_hour_day = 0.0d,
      sin_minute_hour = 0.0d,
      cos_minute_hour = 0.0d,
      sin_minute_day = 0.0d,
      cos_minute_day = 0.0d,

      DoNotTrack =  Option(DoNotTrackLookupRecord()),
      CreativeId =  Some(""),
      PrivateContractId =  "5416475",
      // advertiser columns
      AdvertiserIndustryCategoryId = Some(math.BigInt.long2bigInt(1l)),

      // contextual cols
      ReferrerUrl = Some(""),
      ContextualCategories = Some(Seq(1L)),

      // seeninbidding columns
      ThirdPartyTargetingDataIds = Option(Array(1L)),
      GroupThirdPartyTargetingDataIds = Option(Array(1L)),

      // adgroupkoaoptimizationsettings
      IsAdFormatOptimizationEnabled = Option(false),
      IsGeoSegmentOptimizationEnabled = Option(false),
      KoaCanBidUpEnabled = Option(false), // assist only with performance
      IsEnabled = Option(false),    //koa always on or always off
      UserSegmentCount = Option(1000),

      MatchedSegments = Seq[Long](),
      ExpectedValue = Option(200d),
      RPacingValue = Option(0.5),
      JanusVariantMap = JanusVariantMap,
      ModelVersionsUsed = ModelVersionsUsed,
      UserAgeInDays = Option(20),

      AdInfoSpoInventoryIdHash = Some(0L),
      AdInfoSpoFilteredStatusId = Some(0)
    )

  val privateContractsMock = PrivateContractRecord(
    PrivateContractId = "5416475",
    OwnerPartnerId = "asdkmads",
    Name = "TestPCid",
    Description = None,
    StartDateUtc = None,
    EndDateUtc = None,
    LogoUrl = None,
    AreGuaranteedTermsAllowed = false,
    IsArchived = false,
    PublisherRelationshipTypeId = 1,
    PublisherDirectPrivateContractId = None,
    IsAvailableToRecommendationEngine = false,
    CreatedBy = None,
    CreatedOn = None,
    ApprovedBy = None,
    ApprovedOn = None,
    PrivateContractBillsTTD = false,
    TTDBillingApprovalStateId = 2,
    PMPMarketplaceOrderId = None,
    HasFillRateGoal = false,
    CurrencyCodeId = "USD",
    IsProgrammaticGuaranteedContract = false,
    BiddingPriority = None,
    GdprExemptionFlags = None,
    IsProgrammaticGuaranteedV2 = false,
    IsDecisionedProgrammaticGuaranteed = false,
    TimeZoneId = "UTC",
    IsLiveEvent = false
  )

  val adFormatMock = AdFormatRecord(
    AdFormatId = "2",
    IABName = "Square Pop-Up",
    WidthInPixels = 250,
    HeightInPixels = 250,
    IsRtbEligible = true,
    IsDisplaySupported = true,
    IsVideoSupported = false,
    DisplayNameShort = Some("250x250"),
    IsOutlookEligible = false,
    IsFacebookRightHandSideSupported = false,
    IsFacebookPagePostSupported = false,
    IsFacebookEligible = Some(false),
    IsMobileSupported = false,
    IsAppleIAdSupported = false,
    IsTvSupported = false,
    IsCommonFormat = false,
    MediaTypeId = MediaTypeId.Display,
    UserFriendlyLabel = None
  )

  val pcResultsLogMock = PlutusLogsData(
    BidRequestId = "1",
    InitialBid = 10,
    FinalBidPrice = 9,
    Discrepancy = 0.1,
    BaseBidAutoOpt = 0.3,
    LegacyPcPushdown = 0,
    Mu = 0.2f,
    Sigma = 0.1f,
    GSS = 3.0,
    AlternativeStrategyPush = 1.0,
    Model = "plutus",
    Strategy = 0,
    OptOutDueToFloor = false,
    FloorPrice = 1,
    PartnerSample = false,
    BidBelowFloorExceptedSource = 0,
    FullPush = false,
    UseUncappedBidForPushdown = true,
    UncappedFirstPriceAdjustment = 1.023,
  )

  val pcResultsRawLogMock = PcResultsRawLogs(
    BidRequestId = "1",
    InitialBid = 10,
    FinalBidPrice = 9,
    Discrepancy = 0.1,
    BaseBidAutoOpt = 0.3,
    LegacyPcPushdown = 0,
    PlutusLog = PlutusLog (0.2f, 0.1f, 3, 1),
    PredictiveClearingStrategy = PredictiveClearingStrategy ("plutus", 0),
    OptOutDueToFloor = false,
    FloorPrice = 1,
    PartnerSample = false,
    BidBelowFloorExceptedSource = 0,
    FullPush = false,
    FloorBufferAdjustment = 0,
    UseUncappedBidForPushdown = false,
    UncappedFirstPriceAdjustment = 2.789,
    LogEntryTime = 638560527380000000L
  )

  val mbtwDataMock = MinimumBidToWinData(
    BidRequestId = "1",
    SupplyVendorLossReason = 2,
    LossReason = 2,
    WinCPM = 2.2,
    mbtw = 2
  )

  val productionAdgroupBudgetMock = ProductionAdgroupBudgetData(
    AdGroupId = "fakeadgroup123",
    IsValuePacing = Some(true),
    IsUsingPIDController = Some(false)
  )

  val feeFeatureUsageLogMock = FeeFeatureUsageLogBackingData(
      FeeFeatureType = FeeFeatureLookupRecord(41),
      FeeAmount = 0.000012,
      MarginType = FeeFeatureMarginTypeLookupRecord(1),
      PassThroughFeeCardId = None,
      PassThroughFeeId = None,
      IsMargin = false
  )

  def pcResultsMergedMock(dealId: String = "", fpa: Option[Double] = Some(0.73), campaignId: Option[String] = Some("jkl789"), supplyVendor: Option[String] = Some("google"), pcMode: Int = 3, channel: String = "MobileInApp", isImp: Boolean = true, feeAmount: Option[Double] = Some(0.000012), baseBidAutoOpt: Double = 1, finalBidPrice: Double = 36, floorPrice: Double = 5, mu: Float = 0.5f, sigma: Float = 2.5f, model: String = "plutus", strategy: Int = 100, useUncappedBidForPushdown: Boolean = false, uncappedFpa: Double = 0) = PcResultsMergedDataset(
    BidRequestId = "1",
    DealId = dealId,

    UIID = Some("000"),

    AdjustedBidCPMInUSD = 50.0,
    BidsFirstPriceAdjustment = fpa, //Some(0.73),
    FloorPriceInUSD = Some(5.0),

    PartnerId = Some("abc123"),
    AdvertiserId = Some("def456"),
    CampaignId = campaignId,
    AdGroupId = Some("mno012"),

    SupplyVendor = supplyVendor,
    SupplyVendorPublisherId = Some("983"),
    AliasedSupplyPublisherId = Some(1),
    SupplyVendorSiteId = Some(""),
    Site = Some(""),
    ImpressionPlacementId = Some(""),
    AdWidthInPixels = 250,
    AdHeightInPixels = 250,

    MatchedCategoryList = Some(List("")),
    MatchedFoldPosition = 1,
    RenderingContext = 1,
    ReferrerCategories = Seq(""),

    VolumeControlPriority = Some(1),
    VolumeControlPriorityKeepRate = Some(50.0),
    LogEntryTime = java.sql.Timestamp.valueOf("2020-06-05 18:48:05.123"),

    AdsTxtSellerType = 1,
    PublisherType = 5,
    AuctionType = Some(1),

    Country = Some(""),
    Region = Some(""),
    Metro = Some(""),
    City = Some(""),
    Zip = Some(""),

    DeviceType = 1,
    DeviceMake = Some(""),
    DeviceModel = Some(""),
    OperatingSystem = 1,
    OperatingSystemFamily = 2,
    Browser = 2,
    InternetConnectionType = 1,

    UserHourOfWeek = Some(0),
    RequestLanguages = "",
    MatchedLanguageCode = Some(""),
    Latitude = Some(0.0d),
    Longitude = Some(0.0d),

    PredictiveClearingMode = pcMode,//3,
    PredictiveClearingRandomControl = false,

    // bidfeedback cols
    MediaCostCPMInUSD = Some(37.0),
    DiscrepancyAdjustmentMultiplier = Some(1.0),
    AdvertiserCostInUSD = Some(0.1),
    PartnerCostInUSD = Some(0.1),
    TTDCostInUSD = Some(0.05),
    AdvertiserCurrencyExchangeRateFromUSD = None,

    SubmittedBidAmountInUSD = 0.05,
    ImpressionsFirstPriceAdjustment = Some(0.75),

    // computed columns
    IsImp = isImp,//true,

    sin_hour_week = 0.0d,
    cos_hour_week = 0.0d,
    sin_hour_day = 0.0d,
    cos_hour_day = 0.0d,
    sin_minute_hour = 0.0d,
    cos_minute_hour = 0.0d,
    sin_minute_day = 0.0d,
    cos_minute_day = 0.0d,

    DoNotTrack = 1,
    CreativeId = Some(""),

    PrivateContractId = "5416475",

    FeeAmount = feeAmount,//Some(0.000012), // Fee we charge our users for PC

    MediaTypeId = 1, // from AdFormatDataSet
    Channel = channel,//"MobileInApp",
    ChannelSimple = "Display",

    DetailedMarketType = "Open Market", // from PrivateContractRecord

    JanusVariantMap = None,
    IsUsingJanus = false,

    PlutusVersionUsed = None,

    // Coalesced AliasedSupplyPublisherId and SupplyVendorPublisherId
    AspSvpId = "1983",

    // Fields from PCResults Log Dataset
    InitialBid = 50,
    FinalBidPrice = finalBidPrice,//36,
    Discrepancy = 1.03,
    BaseBidAutoOpt = baseBidAutoOpt, //1,
    OptOutDueToFloor = false,
    FloorPrice = floorPrice, //5,
    PartnerSample = false,
    BidBelowFloorExceptedSource = 0,
    FullPush = false,

    // Fields From PlutusLog
    Mu = mu, //0.5f,
    Sigma = sigma,
    GSS = 0.5,
    AlternativeStrategyPush = 1.0,

    // Fields from PredictiveClearingStrategy
    Model = model,
    Strategy = strategy,
    LossReason = 1, // from MinimumBidToWin data
    WinCPM = 1.0d, // from MinimumBidToWin data
    mbtw = 20.0d,//1.0d, // from MinimumBidToWin data

    isMbtwValidStrict = false,
    isMbtwValid = false,

    // User Data Fields

    UserSegmentCount = Some(1000),
    UserAgeInDays = Some(20),
    MatchedSegments = Seq(1000L),

    // Value Pacing Fields

    ExpectedValue = Some(200),
    RPacingValue = Some(0.5),

    IsValuePacing = Some(true), // from ProductionAdgroupBudgetData
    IsUsingPIDController = Some(false),

    UseUncappedBidForPushdown = useUncappedBidForPushdown,
    UncappedFirstPriceAdjustment = uncappedFpa
  )

  def supplyVendorMock(supplyVendorName: String = "google", openPathEnabled: Boolean = false) = SupplyVendorRecord(
    SupplyVendorId = 192,
    SupplyVendorName = supplyVendorName,//"cafemedia",
    PermissionDefault = true,
    PermissionOverride = false,
    AllowNielsenGRPTracking = true,
    AllowPrivateContracts = true,
    PrivateDealPatternRegex = Some(""),
    PrivateDealPatternDescription = Some(""),
    DefaultSeatId = Some(""),
    PercentagOfFeedbackEligibleForCookieMapping = 0.0,
    DefaultCookiePartnerGroupPermission = false,
    MaxNumberOfTtdInitiatedCookieMappingRedirects = 0,
    AllowBidRequestDataUsage = false,
    UseNameOverrideForReporting = true,
    SupplyVendorNameOverride = Some(""),
    IsWhiteOpsVerified = true,
    RequiresCreativeDealApproval = false,
    PriorityTier = 1,
    IsProgrammaticGuaranteedCertified = false,
    IsForAdsTxtMappingOnly = false,
    AllowInternalDataUsage = true,
    CanFrequencyCapPGContracts = false,
    IsTrafficFromChinaDataCenter = false,
    RequireChinaMSA = false,
    SupplyPublisherId = Some(1000L),
    IsAllowedCustomBidder = false,
    CustomBidderDefaultPercent = 1.0,
    AvailsStreamLoggingEnabled = true,
    AvailsStreamLogRate = 1.0,
    OpenPathEnabled = openPathEnabled
  )

  val adGroupMock = AdGroupRecord(
    AdGroupId = "mno012",
    AdGroupName = "test_ag",
    CampaignId = "jkl789",
    AudienceId = Some("12345"),
    PacingGrainId = 1,
    ExcludeLowValueUsers = Some(false),
    TargetHighValueUsers = Some(false),
    ROIGoalTypeId = 2,
    BaseBidCPMInAdvertiserCurrency = Some(10.0),
    MaxBidCPMInAdvertiserCurrency = Some(20.0),
    TargetPerGrainInAdvertiserCurrency = Some(25.0),
    ROIGoalValue = Some(50.0),
    PrivateContractDefaultAdjustment = Some(2.0),
    PredictiveClearingEnabled = true,
    AdvertiserId = Some("def456"),
    PartnerId = Some("abc123")
  )

  val campaignMock = CampaignRecord(
    CampaignId = Some("jkl789"),
    CampaignName = Some("test_cp"),
    AdvertiserId = Some("def456"),
    CustomCPATypeId = Some(1),
    CustomROASTypeId = Some(1),
    StackRankingEnabled = Some(false),
    DailyTargetInAdvertiserCurrency = Some(5000.0),
    CustomCPAClickWeight = Some(5.0),
    CustomCPAViewthroughWeight = Some(5.0),
    IsManagedByTTD = Some(false)
  )

  val roiGoalTypeMock = ROIGoalTypeRecord(
    ROIGoalTypeId = 2,
    ROIGoalTypeName = "Reach"
  )

  def campaignAdjustmentsPacingMock(campaignId: String = "campaign1", campaignFlightId: Option[Long] = Some(1000000L), adjustment: Double = 0.75, istest: Option[Boolean] = Some(true), enddate: Timestamp = Timestamp.valueOf(LocalDateTime.of(2024, 12, 1, 14, 30)), pacing: Option[Int] = Some(1), improvedNotPacing: Option[Int] = Some(0), worseNotPacing: Option[Int] = Some(0)
                             ): Dataset[CampaignAdjustmentsPacingSchema] = {
    Seq(CampaignAdjustmentsPacingSchema(
      CampaignId = campaignId,
      CampaignFlightId = campaignFlightId,
      CampaignPCAdjustment = adjustment,
      IsTest = istest,
      AddedDate = Some(LocalDate.of(2024, 5, 10)),
      EndDateExclusiveUTC = enddate,
      IsValuePacing = Some(true),
      Pacing = pacing,
      ImprovedNotPacing = improvedNotPacing,
      WorseNotPacing = worseNotPacing,
      MinCalculatedCampaignCapInUSD = 1000,
      MaxCalculatedCampaignCapInUSD = 1000,
      OverdeliveryInUSD = 0,
      UnderdeliveryInUSD = 10,
      EstimatedBudgetInUSD = 1000.0,
      TotalAdvertiserCostFromPerformanceReportInUSD = 1000.0,
      UnderdeliveryFraction = 0.5
    )).toDS()
  }

  def campaignFlightMock(campaignFlightId:Long = 1122330L, campaignId:String = "newcampaign1", enddate: Timestamp = Timestamp.valueOf(LocalDateTime.of(2024, 12, 1, 14, 30)), isCurrent: Int = 1
                        ): Dataset[CampaignFlightRecord]= {
    Seq(CampaignFlightRecord(
      CampaignFlightId = campaignFlightId,
      CampaignId = campaignId,
      EndDateExclusiveUTC = enddate,
      BudgetInAdvertiserCurrency = 2000,
      IsCurrent = isCurrent
    )).toDS()
  }

  def campaignUnderdeliveryMock(date: Timestamp = Timestamp.valueOf(LocalDateTime.of(2024, 6, 25, 0, 0)), campaignId: String = "newcampaign1", campaignFlightId: Integer = 1122330, underdelivery: Double = 4100, spend: Double = 900, cappedpotential:Double = 4500, fraction: Double = 0.8
                               ): Dataset[CampaignThrottleMetricSchema] = {
    Seq(CampaignThrottleMetricSchema(
      Date = date,
      CampaignId = campaignId,
      CampaignFlightId = campaignFlightId,
      IsValuePacing = true,
      //TestBucket = 100L,
      MinCalculatedCampaignCapInUSD = 1000,
      MaxCalculatedCampaignCapInUSD = 1000,
      OverdeliveryInUSD = 0,
      UnderdeliveryInUSD = underdelivery,
      //CappedUnderdeliveryInUSD = cappedunderdelivery,
      TotalAdvertiserCostFromPerformanceReportInUSD = spend,
      //DailyAdvertiserCostInUSD = spend,
      //TargetSpendUSD = target,
      EstimatedBudgetInUSD = cappedpotential,
      UnderdeliveryFraction = fraction
    )).toDS()
  }

  def platformReportMock(country: Option[String] = Some("Canada"), campaignId: Option[String] = Some("newcampaign1"), imps: Option[Long] = Some(120000), pcsavings: Option[BigDecimal] = Some(5.0)): Dataset[RtbPlatformReportCondensedData] = {
    Seq(RtbPlatformReportCondensedData(
      Country = country,
      RenderingContext = Some("1"),
      DeviceType = Some("4"),
      AdFormat = Some("250x250"),
      CampaignId = campaignId,
      BidCount = Some(1500000),
      ImpressionCount = imps,
      BidAmountInUSD = Some(10.0),
      MediaCostInUSD = Some(1000),
      AdvertiserCostInUSD = Some(1000),
      PartnerCostInUSD = Some(1000),
      PredictiveClearingSavingsInUSD = pcsavings,
      TTDMarginInUSD = Some(1000)
    )).toDS()
  }

  val platformWideStatsMock = PlatformWideStatsSchema(
    ContinentalRegionId = Some(1),
    ChannelSimple = "Display",
    Avg_WinRate = 0.12672,
    Med_WinRate = 0.09597,
    TotalSpend = 1000,
    TotalPredictiveClearingSavings = 10,
    Avg_PredictiveClearingSavings = 10,
    Med_PredictiveClearingSavings = 10,
    Avg_FirstPriceAdjustment = 0.70629,
    Med_FirstPriceAdjustment = 0.69913
  )

  def countryMock(countryid: String = "f5k5pffi9w", short: String = "CA", long: String = "Canada", continentalregionid: Option[Int] = Some(1)
                 ): Dataset[CountryRecord] = {
    Seq(CountryRecord(
      CountryId = countryid,
      ShortName = short,
      LongName = long,
      Targetable = false,
      MaxMindLocationId = Some(6251999),
      Iso3 = Some("CAN"),
      NumCode = Some(124),
      PhoneCode = Some(1),
      ContinentalRegionId = continentalregionid,
      GenerateOpenMarketWinRate = true
    )).toDS()
  }

}
