package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.ttdEnv

final case class FirstPartyPixelModelInputRecord(
                                                  SupplyVendor: Option[Int],
                                                  SupplyVendorPublisherId: Option[Int],
                                                  AliasedSupplyPublisherId: Option[Int],
                                                  Site: Option[Int],
                                                  Country: Option[Int],
                                                  Region: Option[Int],
                                                  City: Option[Int],
                                                  Zip: Option[Int],
                                                  DeviceMake: Option[Int],
                                                  DeviceModel: Option[Int],
                                                  RequestLanguages: Int,
                                                  RenderingContext: Option[Int],
                                                  DeviceType: Option[Int],
                                                  OperatingSystemFamily: Option[Int],
                                                  OperatingSystem: Option[Int],
                                                  MatchedFoldPosition: Option[Int],
                                                  InternetConnectionType: Option[Int],
                                                  Browser: Option[Int],
                                                  TargetingDataId: Int,
                                                  AdWidthInPixels: Double,
                                                  AdHeightInPixels: Double,

                                                  sin_hour_week: Double,
                                                  cos_hour_week: Double,
                                                  sin_hour_day: Double,
                                                  cos_hour_day: Double,
                                                  sin_minute_hour: Double,
                                                  cos_minute_hour: Double,
                                                  sin_minute_day: Double,
                                                  cos_minute_day: Double,

                                                  Latitude: Double,
                                                  Longitude: Double,

                                                  Target: Double,

                                                  TDID: String,
                                                  BidRequestId: String,
                                                  AdvertiserId: Option[Int],
                                                  CampaignId: String,
                                                  AdGroupId: String,

                                                  IsPrimaryTDID: Int
                                                )

case class FirstPartyPixelModelInputDataset(tag: String, version: Int) extends
  LightWritableDataset[FirstPartyPixelModelInputRecord](s"/${ttdEnv}/audience/firstPartyPixel/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, 100)
