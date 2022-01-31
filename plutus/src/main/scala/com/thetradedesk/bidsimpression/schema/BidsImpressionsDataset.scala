package com.thetradedesk.bidsimpression.schema

import com.thetradedesk.plutus.data.schema.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord}

case class BidsImpressionsSchema(
                                 // bidrequest cols
                                  BidRequestId: String,
                                  DealId: String,

                                  AdjustedBidCPMInUSD: BigDecimal,
                                  BidsFirstPriceAdjustment: Option[BigDecimal],
                                  FloorPriceInUSD: Option[BigDecimal],

                                  PartnerId: Option[String],
                                  AdvertiserId: Option[String],
                                  CampaignId: Option[String],
                                  AdGroupId: Option[String],

                                  SupplyVendor: Option[String],
                                  SupplyVendorPublisherId: Option[String],
                                  SupplyVendorSiteId: Option[String],
                                  Site: Option[String],
                                  ImpressionPlacementId: Option[String],
                                  AdWidthInPixels: Int,
                                  AdHeightInPixels: Int,

                                  MatchedCategory: Option[String],
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
                                  OperatingSystemFamily: Option[OSFamilyLookupRecord],
                                  Browser: Option[BrowserLookupRecord],

                                  UserHourOfWeek: Option[Int],
                                  RequestLanguages: String, MatchedLanguageCode: Option[String],
                                  Latitude: Option[Double],
                                  Longitude: Option[Double],

                                  PredictiveClearingMode: Option[PredictiveClearingModeLookupRecord] = None,
                                  PredictiveClearingRandomControl: Boolean = false,

                                  // bidfeedback cols

                                  MediaCostCPMInUSD: Option[BigDecimal],
                                  DiscrepancyAdjustmentMultiplier: Option[BigDecimal],

                                  SubmittedBidAmountInUSD: BigDecimal,
                                  ImpressionsFirstPriceAdjustment: Option[BigDecimal],

                                  IsImp: Boolean
                                )

object BidsImpressions {
  val BIDSIMPRESSIONSS3: String = "s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/"
}