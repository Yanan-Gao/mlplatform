package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.shared.schemas.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, InternetConnectionRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord, OSLookupRecord}
import com.thetradedesk.spark.util.TTDConfig.config

case class BidsImpressionsSchema(
                                  // bidrequest cols
                                  BidRequestId: String,

                                  UIID : Option[String],

                                  PartnerId: Option[String],
                                  AdvertiserId: Option[String],
                                  CampaignId: Option[String],
                                  AdGroupId: Option[String],

                                  SupplyVendor: Option[String],
                                  SupplyVendorPublisherId: Option[String],
                                  Site: Option[String],
                                  AdWidthInPixels: Int,
                                  AdHeightInPixels: Int,

                                  MatchedFoldPosition: Int,
                                  RenderingContext: Option[RenderingContextLookupRecord],

                                  LogEntryTime: java.sql.Timestamp,

                                  Country: Option[String],
                                  Region: Option[String],
                                  City: Option[String],
                                  Zip: Option[String],

                                  DeviceType: Option[DeviceTypeLookupRecord],
                                  DeviceMake: Option[String],
                                  DeviceModel: Option[String],
                                  OperatingSystem: Option[OSLookupRecord],
                                  OperatingSystemFamily: Option[OSFamilyLookupRecord],
                                  Browser: Option[BrowserLookupRecord],
                                  InternetConnectionType: Option[InternetConnectionRecord],

                                  RequestLanguages: String, MatchedLanguageCode: Option[String],
                                  Latitude: Option[Double],
                                  Longitude: Option[Double],

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

                                  // contextual cols
                                  ContextualCategories: Option[Seq[Long]],
                                )

case class DailyBidsImpressionsDataset(experimentName: String = "") extends KongMingDataset[BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1",
  experimentName = config.getString("ttd.DailyBidsImpressionsDataset.experimentName", experimentName)
)
