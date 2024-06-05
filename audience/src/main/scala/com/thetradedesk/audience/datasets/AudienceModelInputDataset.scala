package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.audience.{audienceResultCoalesce, audienceVersionDateFormat, ttdEnv, getClassName}

final case class AudienceModelInputRecord(
                                                  SupplyVendor: Option[Int],
                                                  SupplyVendorPublisherId: Option[Int],
                                                  AliasedSupplyPublisherId: Option[Int],
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
                                                  ContextualCategoriesTier1: Array[Int],
                                                  MatchedSegments: Array[Long],
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
                                                  Targets: Seq[Float],
                                                  TDID: String,
                                                  BidRequestId: String,
                                                  AdvertiserId: Option[Int],
                                                  CampaignId: String,
                                                  AdGroupId: String,
                                                  GroupId: String
                                                )

case class AudienceModelInputDataset(model: String, tag: String, version: Int = 1) extends
  LightWritableDataset[AudienceModelInputRecord](s"/${ttdEnv}/audience/${model}/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
object Schedule extends Enumeration {
  type Schedule = Value
  val None, Full, Incremental = Value
}

object IncrementalTrainingTag extends Enumeration {
  type IncrementalTrainingTag = Value
  val None, Full, Small, New = Value
}