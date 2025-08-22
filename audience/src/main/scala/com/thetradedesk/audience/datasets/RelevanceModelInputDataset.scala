package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.{audienceResultCoalesce, audienceVersionDateFormat, ttdWriteEnv}

final case class RelevanceModelInputRecord(
                                           InternetConnectionType: Option[Int],
                                           SplitRemainder: Int,
                                           sin_hour_week: Float,
                                           cos_hour_week: Float,
                                           sin_hour_day: Float,
                                           cos_hour_day: Float,
                                           sin_minute_hour: Float,
                                           cos_minute_hour: Float,
                                           sin_minute_day: Float,
                                           cos_minute_day: Float,
                                           Latitude: Float,
                                           Longitude: Float,
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
                                           SampleWeights: Seq[Float],
                                           MatchedSegments: Array[Long],
                                           MatchedSegmentsLength: Float,
                                           HasMatchedSegments: Option[Int],
                                           UserSegmentCount: Float,
                                           IdTypesBitmap: Integer,
                                           TDID: String,
                                           IDType: Int,
                                           BidRequestIdmostSigBits: Long,
                                           BidRequestIdleastSigBits: Long,
                                           TDIDmostSigBits: Long,
                                           TDIDleastSigBits: Long
                                         )

case class RelevanceModelInputDataset(model: String, tag: String, version: Int = 1) extends
  LightWritableDataset[RelevanceModelInputRecord](s"/${ttdWriteEnv}/audience/${model}/${tag}/v=${version}", S3Roots.ML_PLATFORM_ROOT, audienceResultCoalesce, dateFormat = audienceVersionDateFormat)
