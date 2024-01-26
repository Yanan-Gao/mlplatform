package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.{paddedDatePart, paddedDateTimePart}
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.streaming.records.rtb.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, InternetConnectionTypeLookupRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, OSLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord}
import job.PcResultsGeronimoJob.ttdEnv

import java.time.LocalDateTime

case class PcResultsMergedSchema(
    // bidrequest cols
    BidRequestId: String,
    DealId: String,

    UIID : Option[String],

    AdjustedBidCPMInUSD: BigDecimal,
    BidsFirstPriceAdjustment: Option[BigDecimal],
    FloorPriceInUSD: Option[BigDecimal],

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
    RenderingContext: Option[RenderingContextLookupRecord],
    ReferrerCategories: Seq[String],

    VolumeControlPriority: Option[Int],
    LogEntryTime: java.sql.Timestamp,


    AdsTxtSellerType: Option[AdsTxtSellerTypeLookupRecord],
    PublisherType: InventoryPublisherTypeLookupRecord,
    AuctionType: Option[Int],

    Country: Option[String],
    Region: Option[String],
    Metro: Option[String],
    City: Option[String],
    Zip: Option[String],

    DeviceType: Option[DeviceTypeLookupRecord],
    DeviceMake: Option[String],
    DeviceModel: Option[String],
    OperatingSystem: Option[OSLookupRecord],
    OperatingSystemFamily: Option[OSFamilyLookupRecord],
    Browser: Option[BrowserLookupRecord],
    InternetConnectionType: Option[InternetConnectionTypeLookupRecord],

    UserHourOfWeek: Option[Int],
    RequestLanguages: String, MatchedLanguageCode: Option[String],
    Latitude: Option[Double],
    Longitude: Option[Double],

    PredictiveClearingMode: Option[PredictiveClearingModeLookupRecord] = None,
    PredictiveClearingRandomControl: Boolean = false,
    PlutusTfModel: Option[String],

    // bidfeedback cols
    MediaCostCPMInUSD: Option[BigDecimal],
    DiscrepancyAdjustmentMultiplier: Option[BigDecimal],

    SubmittedBidAmountInUSD: BigDecimal,
    ImpressionsFirstPriceAdjustment: Option[BigDecimal],

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

    DoNotTrack: Option[DoNotTrackLookupRecord],
    CreativeId: Option[String],

    PrivateContractId: String,

    // Note: I've removed a bunch of fields from the Geronimo schema since
    // these fields dont seem relevant to plutus.

    // Fields from PCResults Log Dataset
    InitialBid: Double,
    FinalBidPrice: Double,
    Discrepancy: Double,
    BaseBidAutoOpt: Double,
    LegacyPcPushdown: Double,
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
    Strategy: Int,

    // Fields from MinimumBidToWin data
    SupplyVendorLossReason: Int,
    LossReason: Int,
    WinCPM: Double,
    mbtw: Double
)

object PcResultsMergedDataset {
  val S3PATH = f"${S3Roots.IDENTITY_ROOT}/${ttdEnv}/pcresultsgeronimo/v=2/"
  def S3PATH_GEN = (dateTime: LocalDateTime) => paddedDateTimePart(dateTime)
}
