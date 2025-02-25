package com.thetradedesk.kongming.datasets

final case class TrainSetFeaturesRecord (

                                          AdGroupId: String,
                                          BidRequestId: String,
                                          CampaignId: String,
                                          AdvertiserId: String,

                                          AdGroupIdEncoded: Long,
                                          CampaignIdEncoded: Long,
                                          AdvertiserIdEncoded: Long,

                                          LogEntryTime: String,
                                          Weight: Double,
                                          Target: Int,
                                          Revenue: Option[BigDecimal],
                                          IsInTrainSet: Boolean,
                                          IsTracked: Int,

                                          SupplyVendor: Option[String],
                                          SupplyVendorPublisherId: Option[String],
                                          AliasedSupplyPublisherId: Option[Int],
                                          Site: Option[String],
                                          ImpressionPlacementId: Option[String],
                                          AdFormat: String,

                                          Country: Option[String],
                                          Region: Option[String],
                                          City: Option[String],
                                          Zip: Option[String],

                                          DeviceMake: Option[String],
                                          DeviceModel: Option[String],
                                          RequestLanguages: String,

                                          RenderingContext: Option[Int],
                                          DeviceType: Option[Int],
                                          OperatingSystem: Option[Int],
                                          Browser: Option[Int],
                                          InternetConnectionType: Option[Int],
                                          MatchedFoldPosition: Int,

                                          HasContextualCategoryTier1: Int,
                                          ContextualCategoryNumberTier1: Int,
                                          ContextualCategoryLengthTier1: Double,
                                          ContextualCategoriesTier1: Array[Int],


                                          HasUserData: Int,
                                          UserData: Array[Long],
                                          UserDataLength: Double,
                                          UserDataOptIn: Int,

                                          sin_hour_week: Double,
                                          cos_hour_week: Double,
                                          sin_hour_day: Double,
                                          cos_hour_day: Double,
                                          sin_minute_hour: Double,
                                          cos_minute_hour: Double,
                                          latitude: Option[Double],
                                          longitude: Option[Double],
                                          IndustryCategoryId: Option[Int],
                                          AudienceId: Array[Int]
                                        )
