package com.thetradedesk.featurestore.datasets

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema


case class FeatureBidsImpression(
                                  // bidrequest cols
                                  BidRequestId: String,
                                  DealId: String,

                                  UIID : Option[String],
                                  IsTracked: Int,

                                  AdjustedBidCPMInUSD: BigDecimal,
                                  BidsFirstPriceAdjustment: Option[BigDecimal],
                                  FloorPriceInUSD: Option[BigDecimal],

                                  PartnerId: Option[String],
                                  AdvertiserId: Option[String],
                                  CampaignId: Option[String],
                                  AdGroupId: Option[String],

                                  SupplyVendorPublisherId: Option[String],
                                  AliasedSupplyPublisherId: Option[Int],
                                  SupplyVendorSiteId: Option[String],
                                  Site: Option[String],
                                  ImpressionPlacementId: Option[String],
                                  AdFormat: String,

                                  MatchedCategoryList: Option[List[String]],
                                  MatchedFoldPosition: Int,
                                  RenderingContext: Option[String],
                                  ReferrerCategories: Seq[String],

                                  VolumeControlPriority: Option[Int],
                                  LogEntryTime: java.sql.Timestamp,

                                  Country: Option[String],
                                  Region: Option[String],
                                  Metro: Option[String],
                                  City: Option[String],
                                  Zip: Option[String],

                                  DeviceType: Option[String],
                                  DeviceMake: Option[String],
                                  DeviceModel: Option[String],
                                  OperatingSystem: Option[String],
                                  OperatingSystemFamily: Option[String],
                                  Browser: Option[String],
                                  InternetConnectionType: Option[String],

                                  UserHourOfWeek: Option[Int],
                                  RequestLanguages: String,
                                  MatchedLanguageCode: Option[String],
                                  Latitude: Option[Double],
                                  Longitude: Option[Double],

                                  // bidfeedback cols
                                  MediaCostCPMInUSD: Option[BigDecimal],
                                  DiscrepancyAdjustmentMultiplier: Option[BigDecimal],

                                  SubmittedBidAmountInUSD: BigDecimal,
                                  ImpressionsFirstPriceAdjustment: Option[BigDecimal],

                                  BillingEventId: Option[Long],

                                  // computed columns
                                  IsImp: Boolean,

                                  sin_hour_week: Double,
                                  cos_hour_week: Double,
                                  sin_hour_day: Double,
                                  cos_hour_day: Double,
                                  sin_minute_hour: Double,
                                  cos_minute_hour: Double,
                                  sin_minute_day: Double,
                                  cos_minute_day: Double,

                                  CreativeId: Option[String],
                                  PrivateContractId: String,

                                  // advertiser columns
                                  AdvertiserIndustryCategoryId: Option[BigInt],

                                  // contextual cols
                                  ReferrerUrl: Option[String],
                                  ContextualCategories: Option[Seq[Long]],

                                  // seeninbidding columns (10% of user/device ids sampled)
                                  ThirdPartyTargetingDataIds: Option[Array[Long]] = None,

                                  // seeninbidding group columns (10% of person ids sampled)
                                  GroupThirdPartyTargetingDataIds: Option[Array[Long]] = None,

                                  ExpectedValue: Option[BigDecimal],
                                  RPacingValue: Option[BigDecimal],

                                  UserAgeInDays: Option[Double]
                                )


case class ConvertedImpressionDataset(attLookback: Int, experimentOverride: Option[String] = None) extends ProcessedDataset[FeatureBidsImpression] (
  s3DatasetPath = s"dailyconvertedimpressions/v=1/lookback=${attLookback}d",
  experimentOverride = experimentOverride
)

