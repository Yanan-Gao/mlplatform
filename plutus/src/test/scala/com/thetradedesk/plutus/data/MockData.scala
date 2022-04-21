package com.thetradedesk.plutus.data

import com.thetradedesk.geronimo.bidsimpression.schema._
import com.thetradedesk.geronimo.shared.schemas._
import com.thetradedesk.plutus.data.schema._

import java.sql.Timestamp
import java.time.Instant

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
                        mb2w: Double = 1.0d                      ) =
    RawLostBidData(LogEntryTime, BidRequestId, CreativeId, AdGroupId, CampaignId, PrivateContractId, PartnerId, AdvertiserId, CampaignFlightId, SupplyVendorLossReason, LossReason, WinCPM, SupplyVendor, BidRequestTime, mb2w)


  val bidsImpressionsMock = BidsImpressionsSchema(
                                    // bidrequest cols
                                    BidRequestId = "1",
                                    DealId = "",

                                    UIID = Option("000"),

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

                                    DoNotTrack =  Option(DoNotTrackLookupRecord()),
                                    CreativeId =  Some(""),
                                    PrivateContractId =  ""


                                  )


}
