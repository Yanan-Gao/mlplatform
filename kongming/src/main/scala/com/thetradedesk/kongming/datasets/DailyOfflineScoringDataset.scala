package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.TFRecord

//TODO: will need to be able to extend what Yuehan has as final result for trainingset.
final case class DailyOfflineScoringRecord(
                                           AdGroupId: Int,
                                           BidRequestIdStr: String,
                                           AdGroupIdStr: String,
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

case class DailyOfflineScoringDataset() extends KongMingDataset[DailyOfflineScoringRecord](
  s3DatasetPath = "dailyofflinescore/v=1",
  fileFormat = TFRecord.Example
)
