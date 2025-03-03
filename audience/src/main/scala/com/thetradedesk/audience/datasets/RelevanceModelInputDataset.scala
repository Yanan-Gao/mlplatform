package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.audience.{audienceResultCoalesce, audienceVersionDateFormat, ttdEnv, getClassName}

final case class RelevanceModelInputRecord(
                                           TDID: String,
                                           InternetConnectionType: Option[Int],
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
                                           Browser: Option[Int],
                                           OperatingSystemFamily: Option[Int],
                                           OperatingSystem: Option[Int],
                                           RequestLanguages: Int,
                                           ZipSiteLevel_Seed: Seq[Int],
                                           RenderingContext: Option[Int],
                                           DeviceType: Option[Int],
                                           Zip: Option[Int],
                                           DeviceMake: Option[Int],
                                           DeviceModel: Option[Int],
                                           AliasedSupplyPublisherId: Option[Int],
                                           Site: Option[Int],
                                           Region: Option[Int],
                                           City: Option[Int],
                                           Country: Option[Int],
                                           BidRequestId: String,
                                           AdvertiserId: Option[Int],
                                           SyntheticIds: Seq[Int],
                                           Targets: Seq[Float],
                                           MatchedSegments: Array[Long],
                                           MatchedSegmentsLength: Double,
                                           HasMatchedSegments: Option[Int],
                                           UserSegmentCount: Double
                                         )

case class RelevanceModelInputDataset(model: String, tag: String, version: Int = 1) extends
  LightWritableDataset[RelevanceModelInputRecord](s"/${ttdEnv}/audience/${model}/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
