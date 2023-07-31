package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.TFRecord

//TODO: will need to be able to extend what Yuehan has as final result for trainingset.
final case class DailyOfflineScoringRecord(
                                           AdGroupId: Int,
                                           CampaignId: Int,
                                           AdvertiserId: Int,
                                           BidRequestIdStr: String,
                                           AdGroupIdStr: String,
                                           CampaignIdStr: String,
                                           AdvertiserIdStr: String,
                                           IsTracked: Int,
                                           IsUID2: Int,

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

                                           HasContextualCategoryTier1: Int,
                                           ContextualCategoryLengthTier1: Double,
                                           ContextualCategoriesTier1: Array[Int],

                                           sin_hour_week: Double,
                                           cos_hour_week: Double,
                                           sin_hour_day: Double,
                                           cos_hour_day: Double,
                                           sin_minute_hour: Double,
                                           cos_minute_hour: Double,
                                           latitude: Option[Double],
                                           longitude: Option[Double]
                                      )

case class DailyOfflineScoringDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyOfflineScoringRecord](
  s3DatasetPath = "dailyofflinescore/v=1",
  experimentOverride = experimentOverride,
  fileFormat = TFRecord.Example
)
