package com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface

import com.thetradedesk.audience.transform.IDTransform
import org.apache.spark.sql.Dataset


case class OptInSeedRecord(SeedId: String,
                           SyntheticId: Int)

case class BidSideDataRecord(BidRequestId: String,
                             TDID: String,
                             IDType: Int = IDTransform.IDType.Unknown.id,
                             Site: Option[String] = None,
                             Zip: Option[String] = None,
                             DeviceAdvertisingId: Option[String] = None,
                             CookieTDID: Option[String] = None,
                             UnifiedId2: Option[String] = None,
                             EUID: Option[String] = None,
                             IdentityLinkId: Option[String] = None,
                             SplitRemainder: Int = 0,
                             AdvertiserId: Option[String] = None,
                             AliasedSupplyPublisherId: Option[Int] = None,
                             Country: Option[String] = None,
                             DeviceMake: Option[String] = None,
                             DeviceModel: Option[String] = None,
                             RequestLanguages: String = "",
                             RenderingContext: Option[Int] = None,
                             DeviceType: Option[Int] = None,
                             OperatingSystemFamily: Option[Int] = None,
                             Browser: Option[Int] = None,
                             Latitude: Option[Float] = None,
                             Longitude: Option[Float] = None,
                             Region: Option[String] = None,
                             City: Option[String] = None,
                             InternetConnectionType: Option[Int] = None,
                             OperatingSystem: Option[Int] = None,
                             sin_hour_week: Float = 0,
                             cos_hour_week: Float = 0,
                             sin_hour_day: Float = 0,
                             cos_hour_day: Float = 0,
                             sin_minute_hour: Float = 0,
                             cos_minute_hour: Float = 0,
                             sin_minute_day: Float = 0,
                             cos_minute_day: Float = 0,
                             MatchedSegments: Array[Long] = Array(),
                             MatchedSegmentsLength: Float = 0,
                             HasMatchedSegments: Option[Int] = None,
                             UserSegmentCount: Float = 0,
                             IdTypesBitmap: Integer = 0,
                             BidRequestIdmostSigBits: Long = 0L,
                             BidRequestIdleastSigBits: Long = 0L,
                             TDIDmostSigBits: Long = 0L,
                             TDIDleastSigBits: Long = 0L

                            )
case class BidResult(rawBidReqData: Dataset[BidSideDataRecord],
                     bidSideTrainingData: Dataset[BidSideDataRecord])
case class SeedLabelSideDataRecord(BidRequestId: String,
                                   SyntheticIds: Seq[Int],
                                   Targets: Seq[Float],
                                   ZipSiteLevel_Seed: Seq[Int]
                                  )

case class UserPosNegSynIds(BidRequestId: String,
                            TDIDs: Seq[String],
                            SplitRemainder: Int,
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
                                     idType: Int,
                                     SeedIds: Seq[String])