package com.thetradedesk.kongming.datasets

final case class DataForModelTrainingRecord(
                                 AdGroupId: Int,
//                                 BidRequestId: String,
                                 Weight: Double,
                                 Target: Int,

                                 SupplyVendor: Option[Int],
                                 SupplyVendorPublisherId: Option[Int],
                                 Site: Option[Int],
                                 AdFormat: Int,

                                 Country: Option[Int],
                                 Region: Option[Int],
                                 City: Option[Int],
                                 Zip: Option[Int],

                                 DeviceMake: Option[Int],
                                 DeviceModel: Option[Int],
                                 RequestLanguages: Int,

                                 RenderingContext: Option[Int],
                                 DeviceType: Option[Int],
                                 OperatingSystem: Option[Int],
                                 Browser: Option[Int],
                                 InternetConnectionType: Option[Int],
                                 MatchedFoldPosition: Int,

                                 sin_hour_week: Double,
                                 cos_hour_week: Double,
                                 sin_hour_day: Double,
                                 cos_hour_day: Double,
                                 sin_minute_hour: Double,
                                 cos_minute_hour: Double,
                                 Latitude: Option[Double],
                                 Longitude: Option[Double]
                                          )


object DataForModelTrainingDataset extends KongMingDataset[DataForModelTrainingRecord](
  s3DatasetPath = "trainset/v=1",
  defaultNumPartitions = 100
)