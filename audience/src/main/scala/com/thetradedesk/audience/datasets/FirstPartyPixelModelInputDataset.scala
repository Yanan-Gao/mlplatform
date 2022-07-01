package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE}
import com.thetradedesk.audience.transform.FeatureDesc
import com.thetradedesk.audience.ttdEnv

final case class FirstPartyPixelModelInputRecord(
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
                                                  @FeatureDesc("OperatingSystemFamily", "int", 6)
                                                  OperatingSystemFamily: Option[Int],
                                                  @FeatureDesc("Browser", "int", 15)
                                                  Browser: Option[Int],
                                                  @FeatureDesc("TargetingDataId", "long", 2000003)
                                                  TargetingDataId: Int,
                                                  AdWidthInPixels: Option[Int],
                                                  AdHeightInPixels: Option[Int],

                                                  sin_hour_week: Double,
                                                  cos_hour_week: Double,
                                                  sin_hour_day: Double,
                                                  cos_hour_day: Double,
                                                  Latitude: Option[Double],
                                                  Longitude: Option[Double],

                                                  Target: Double,
                                                )

case class FirstPartyPixelModelInputDataset() extends
  LightWritableDataset[FirstPartyPixelModelInputRecord](s"/${ttdEnv}/audience/firstPartyPixel/dailyConversionSample/v=1", S3Roots.ML_PLATFORM_ROOT, 100)