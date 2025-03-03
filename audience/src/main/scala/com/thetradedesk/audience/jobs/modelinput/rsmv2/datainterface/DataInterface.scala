package com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface

import org.apache.spark.sql.Dataset


case class OptInSeedRecord(SeedId: String,
                           SyntheticId: Int)

case class BidSideDataRecord(TDID: String,
                             Site: Option[String],
                             Zip: Option[String],
                             BidRequestId: String,
                             AdvertiserId: Option[String],
                             AliasedSupplyPublisherId: Option[Int],
                             Country: Option[String],
                             DeviceMake: Option[String],
                             DeviceModel: Option[String],
                             RequestLanguages: String,
                             RenderingContext: Option[Int],
                             DeviceType: Option[Int],
                             OperatingSystemFamily: Option[Int],
                             Browser: Option[Int],
                             Latitude: Option[Double],
                             Longitude: Option[Double],
                             Region: Option[String],
                             City: Option[String],
                             InternetConnectionType: Option[Int],
                             OperatingSystem: Option[Int],
                             sin_hour_week: Double,
                             cos_hour_week: Double,
                             sin_hour_day: Double,
                             cos_hour_day: Double,
                             sin_minute_hour: Double,
                             cos_minute_hour: Double,
                             sin_minute_day: Double,
                             cos_minute_day: Double,
                             MatchedSegments: Array[Long],
                             MatchedSegmentsLength: Double,
                             HasMatchedSegments: Option[Int],
                             UserSegmentCount: Double
                            )
case class BidResult(rawBidReqData: Dataset[BidSideDataRecord],
                     bidSideTrainingData: Dataset[BidSideDataRecord])
case class SeedLabelSideDataRecord(TDID: String,
                                   SyntheticIds: Seq[Int],
                                   Targets: Seq[Float],
                                   ZipSiteLevel_Seed: Seq[Int]
                                  )

case class UserPosNegSynIds(TDID: String,
                            PositiveSyntheticIds: Seq[Int],
                            NegativeSyntheticIds: Seq[Int]
                           )

case class SampleIndicatorRecord(SeedId: String,
                                 SyntheticId: Int,
                                 PositiveRandIndicator: Double,
                                 PositiveCount: Long,
                                 NegativeCount: Long,
                                 NegativeRightBoundIndicator: Double
                                )

case class UserSiteZipLevelRecord(
                                   TDID: String,
                                   SyntheticId_Level2: Seq[Int],
                                   SyntheticId_Level1: Seq[Int],
                                 )
case class SiteZipDensityRecord(
                                   Site: Option[String],
                                   Zip: Option[String],
                                   SyntheticId: Int,
                                   score: Double
                                 )

case class RSMV2AggregatedSeedRecord(TDID: String,
                                     SeedIds: Seq[String])