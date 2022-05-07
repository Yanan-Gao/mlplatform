package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.shared.schemas.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord}

final case class TrainSetFeaturesRecord (

                                          AdGroupId: String,
                                          BidRequestId: String,
                                          Weight: Double,
                                          Target: Int,
                                          IsInTrainSet: Boolean,

                                          SupplyVendor: Option[String],
                                          SupplyVendorPublisherId: Option[String],
                                          Site: Option[String],
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
                                          MatchedFoldPosition: Int,

                                          sin_hour_week: Double,
                                          cos_hour_week: Double,
                                          sin_hour_day: Double,
                                          cos_hour_day: Double,
                                          sin_minute_hour: Double,
                                          cos_minute_hour: Double,
                                          Latitude: Option[Double],
                                          Longitude: Option[Double]
//                                          InternetConnectionType
                                        )

