package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core._

final case class UserDataValidationDataForModelTrainingRecord(BidRequestIdStr: String,
                                                      AdGroupIdStr: String,
                                                      CampaignIdStr: String,
                                                      AdvertiserIdStr: String,
                                                      AdGroupIdEncoded: Long,
                                                      CampaignIdEncoded: Long,
                                                      AdvertiserIdEncoded: Long,
                                                      AdGroupId: Int,
                                                      CampaignId: Int,
                                                      AdvertiserId: Int,

                                                      Weight: Float,
                                                      Target: Float,
                                                      Revenue: Option[Float],

                                                      //                                                      ImpressionPlacementId: Option[String],
                                                      SupplyVendor: Option[Int],
                                                      SupplyVendorPublisherId: Option[Int],
                                                      AliasedSupplyPublisherId: Option[Int],
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
                                                      ContextualCategoryLengthTier1: Float,
                                                      ContextualCategoriesTier1: Array[Int],

                                                      sin_hour_week: Float,
                                                      cos_hour_week: Float,
                                                      sin_hour_day: Float,
                                                      cos_hour_day: Float,
                                                      sin_minute_hour: Float,
                                                      cos_minute_hour: Float,
                                                      latitude: Option[Float],
                                                      longitude: Option[Float],
                                                      IndustryCategoryId: Option[Int],
                                                      AudienceId: Array[Int],

                                                      UserData: Array[Int],
                                                      HasUserData: Int,
                                                      UserDataLength: Float,
                                                      UserDataOptIn: Int,

                                                      IdType: Int,
                                                      IdCount: Int,
                                                      UserAgeInDays: Option[Float]
                                                     )
