package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core.{DateHourPartitionedS3DataSet, GeneratedDataSet, Parquet}
import com.thetradedesk.streaming.records.rtb._

case class BidsImpressionsSchema(
                                  // bidrequest cols
                                  BidRequestId: String,

                                  UIID : Option[String],

                                  PartnerId: Option[String],
                                  AdvertiserId: Option[String],
                                  CampaignId: Option[String],
                                  AdGroupId: Option[String],

                                  SupplyVendor: Option[String],
                                  SupplyVendorPublisherId: Option[String],
                                  AliasedSupplyPublisherId: Option[Int],
                                  Site: Option[String],
                                  ImpressionPlacementId: Option[String],
                                  AdWidthInPixels: Int,
                                  AdHeightInPixels: Int,

                                  MatchedFoldPosition: Int,
                                  RenderingContext: Option[RenderingContextLookupRecord],

                                  LogEntryTime: java.sql.Timestamp,

                                  Country: Option[String],
                                  Region: Option[String],
                                  City: Option[String],
                                  Zip: Option[String],

                                  DeviceType: Option[DeviceTypeLookupRecord],
                                  DeviceMake: Option[String],
                                  DeviceModel: Option[String],
                                  OperatingSystem: Option[OSLookupRecord],
                                  OperatingSystemFamily: Option[OSFamilyLookupRecord],
                                  Browser: Option[BrowserLookupRecord],
                                  InternetConnectionType: Option[InternetConnectionTypeLookupRecord],

                                  RequestLanguages: String, MatchedLanguageCode: Option[String],
                                  Latitude: Option[Double],
                                  Longitude: Option[Double],

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

                                  // contextual cols
                                  ContextualCategories: Option[Seq[Long]],

                                  // cold storage total segments
                                  UserSegmentCount: Option[Int],
                                  // hotcache x selected segments log
                                  MatchedSegments: Option[Seq[Long]],
                                  // hotcache x audience segments
                                  UserTargetingDataIds: Option[Array[Long]],

                                  // uncomment for online test backtesting set generation
                                  // ExpectedValue: Option[BigDecimal],
                                  // RPacingValue: Option[BigDecimal],
                                  IdType: Option[String],
                                  IdCount: Int,
                                  UserAgeInDays: Option[Double]
                                )

// For easier feature adding while avoiding rubbish data, we should always write the full dataset, but read only the required fields
case class DailyBidsImpressionsDataset(experimentOverride: Option[String] = None) extends KongMingDataset[BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1",
  experimentOverride = experimentOverride
  ) {
  override protected def getMetastoreTableName: String = "dailybidsimpressions"
}

case class DailyHourlyBidsImpressionsDataset(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[BidsImpressionsSchema](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/dailyhourlybidsimpressions/v=1",
    fileFormat = Parquet,
    experimentOverride = experimentOverride
  ) {
  override protected def getMetastoreTableName: String = "dailyhourlybidsimpressions"

  override protected def supportsMetastorePartition: Boolean = false
}

case class DailyBidsImpressionsFullDataset(experimentOverride: Option[String] = None) extends KongMingDataset[com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema](
  s3DatasetPath = "dailybidsimpressions/v=1",
  experimentOverride = experimentOverride
  ) {
  override protected def getMetastoreTableName: String = "dailybidsimpressions"
}
