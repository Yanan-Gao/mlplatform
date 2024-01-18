package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.{audienceVersionDateFormat, ttdEnv}

final case class AudienceModelMonitoringRecord(
                                           SupplyVendor: Option[Int],
                                           SupplyVendorPublisherId: Option[Int],
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

                                           SyntheticIds: Seq[Int],

                                           BidRequestId: String,
                                           AvailableBidRequestId: String,
                                           OnlineModelScore: Double
                                         )

case class AudienceModelMonitoringDataset(model: String, version: Int = 1) extends
  LightWritableDataset[AudienceModelMonitoringRecord](s"/$ttdEnv/audience/$model/v=$version", S3Roots.ML_PLATFORM_ROOT, defaultNumPartitions = 100, dateFormat = audienceVersionDateFormat)
