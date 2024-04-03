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

                                           ImpressionPlacementId: Option[String],
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
                                           longitude: Option[Double],
                                           IndustryCategoryId: Option[Int],
                                           AudienceId_Column0: Int,
                                           AudienceId_Column1: Int,
                                           AudienceId_Column2: Int,
                                           AudienceId_Column3: Int,
                                           AudienceId_Column4: Int,
                                           AudienceId_Column5: Int,
                                           AudienceId_Column6: Int,
                                           AudienceId_Column7: Int,
                                           AudienceId_Column8: Int,
                                           AudienceId_Column9: Int,
                                           AudienceId_Column10: Int,
                                           AudienceId_Column11: Int,
                                           AudienceId_Column12: Int,
                                           AudienceId_Column13: Int,
                                           AudienceId_Column14: Int,
                                           AudienceId_Column15: Int,
                                           AudienceId_Column16: Int,
                                           AudienceId_Column17: Int,
                                           AudienceId_Column18: Int,
                                           AudienceId_Column19: Int,
                                           AudienceId_Column20: Int,
                                           AudienceId_Column21: Int,
                                           AudienceId_Column22: Int,
                                           AudienceId_Column23: Int,
                                           AudienceId_Column24: Int,
                                           AudienceId_Column25: Int,
                                           AudienceId_Column26: Int,
                                           AudienceId_Column27: Int,
                                           AudienceId_Column28: Int,
                                           AudienceId_Column29: Int
                                          )

case class DailyOfflineScoringDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyOfflineScoringRecord](
  s3DatasetPath = "dailyofflinescore/v=1",
  experimentOverride = experimentOverride,
  fileFormat = TFRecord.Example
)
