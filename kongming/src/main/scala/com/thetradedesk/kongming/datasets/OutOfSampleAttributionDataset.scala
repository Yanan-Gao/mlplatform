package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core._

final case class OutOfSampleAttributionRecord(
                                               AdGroupId: Int,
                                               CampaignId: Int,
                                               AdvertiserId: Int,
                                               BidRequestIdStr: String,
                                               AdGroupIdStr: String,
                                               CampaignIdStr: String,
                                               AdvertiserIdStr: String,
                                               IsTracked: Int,
                                               IsUID2: Int,

                                               Target: Int,
                                               Revenue: Option[BigDecimal],

                                               SupplyVendor: Option[Int],
                                               SupplyVendorPublisherId: Option[Int],
                                               ImpressionPlacementId: Option[String],
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

                                               ContextualCategoriesTier1_Column0: Int,
                                               ContextualCategoriesTier1_Column1: Int,
                                               ContextualCategoriesTier1_Column2: Int,
                                               ContextualCategoriesTier1_Column3: Int,
                                               ContextualCategoriesTier1_Column4: Int,
                                               ContextualCategoriesTier1_Column5: Int,
                                               ContextualCategoriesTier1_Column6: Int,
                                               ContextualCategoriesTier1_Column7: Int,
                                               ContextualCategoriesTier1_Column8: Int,
                                               ContextualCategoriesTier1_Column9: Int,
                                               ContextualCategoriesTier1_Column10: Int,
                                               ContextualCategoriesTier1_Column11: Int,
                                               ContextualCategoriesTier1_Column12: Int,
                                               ContextualCategoriesTier1_Column13: Int,
                                               ContextualCategoriesTier1_Column14: Int,
                                               ContextualCategoriesTier1_Column15: Int,
                                               ContextualCategoriesTier1_Column16: Int,
                                               ContextualCategoriesTier1_Column17: Int,
                                               ContextualCategoriesTier1_Column18: Int,
                                               ContextualCategoriesTier1_Column19: Int,
                                               ContextualCategoriesTier1_Column20: Int,
                                               ContextualCategoriesTier1_Column21: Int,
                                               ContextualCategoriesTier1_Column22: Int,
                                               ContextualCategoriesTier1_Column23: Int,
                                               ContextualCategoriesTier1_Column24: Int,
                                               ContextualCategoriesTier1_Column25: Int,
                                               ContextualCategoriesTier1_Column26: Int,
                                               ContextualCategoriesTier1_Column27: Int,
                                               ContextualCategoriesTier1_Column28: Int,
                                               ContextualCategoriesTier1_Column29: Int,
                                               ContextualCategoriesTier1_Column30: Int,

                                               sin_hour_week: Double,
                                               cos_hour_week: Double,
                                               sin_hour_day: Double,
                                               cos_hour_day: Double,
                                               sin_minute_hour: Double,
                                               cos_minute_hour: Double,
                                               latitude: Option[Double],
                                               longitude: Option[Double],
                                             )

case class OutOfSampleAttributionDataset(delay_n_days: Int, experimentOverride: Option[String] = None)
  extends KongMingDataset[OutOfSampleAttributionRecord](
    s3DatasetPath =  s"outofsampleattributionset/v=1/delay=${delay_n_days}D",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
)
