package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.ttdEnv
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE}
import com.thetradedesk.audience.transform.FeatureDesc

final case class FirstPartyPixelMonitoringRecord(
                                                  @FeatureDesc("SupplyVendor", "string", 102)
                                                  SupplyVendor: Option[Int],
                                                  @FeatureDesc("SupplyVendorPublisherId", "string", 200002)
                                                  SupplyVendorPublisherId: Option[Int],
                                                  @FeatureDesc("Site", "string", 500002)
                                                  Site: Option[Int],
                                                  @FeatureDesc("Country", "string", 252)
                                                  Country: Option[Int],
                                                  @FeatureDesc("Region", "string", 4002)
                                                  Region: Option[Int],
                                                  @FeatureDesc("City", "string", 150002)
                                                  City: Option[Int],
                                                  @FeatureDesc("Zip", "string", 90002)
                                                  Zip: Option[Int],
                                                  @FeatureDesc("DeviceMake", "string", 6002)
                                                  DeviceMake: Option[Int],
                                                  @FeatureDesc("DeviceModel", "string", 40002)
                                                  DeviceModel: Option[Int],
                                                  @FeatureDesc("RequestLanguages", "string", 5002)
                                                  RequestLanguages: Int,
                                                  @FeatureDesc("RenderingContext", "int", 6)
                                                  RenderingContext: Option[Int],
                                                  @FeatureDesc("DeviceType", "int", 9)
                                                  DeviceType: Option[Int],
                                                  @FeatureDesc("OperatingSystemFamily", "int", 8)
                                                  OperatingSystemFamily: Option[Int],
                                                  @FeatureDesc("OperatingSystem", "int", 72)
                                                  OperatingSystem: Option[Int],
                                                  @FeatureDesc("MatchedFoldPosition", "int", 5)
                                                  MatchedFoldPosition: Option[Int],
                                                  @FeatureDesc("InternetConnectionType", "int", 10)
                                                  InternetConnectionType: Option[Int],
                                                  @FeatureDesc("Browser", "int", 16)
                                                  Browser: Option[Int],
                                                  @FeatureDesc("TargetingDataId", "long", 2000003)
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

                                                  BidRequestId: String,
                                                  AvailableBidRequestId: String,

                                                  OnlineModelScore: Double
                                                )

case class FirstPartyPixelMonitoringDataset(tag: String, version: Int) extends
  LightWritableDataset[FirstPartyPixelMonitoringRecord](s"/${ttdEnv}/audience/firstPartyPixel/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, 100)
