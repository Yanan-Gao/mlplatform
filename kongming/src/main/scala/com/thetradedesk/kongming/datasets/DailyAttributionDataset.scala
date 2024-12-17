package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core.{GeneratedDataSet, Parquet}


final case class DailyAttributionRecord(
                                        AttributedEventId: String,
                                        AttributedEventTypeId: String,
                                        ConversionTrackerLogFileId: String,
                                        ConversionTrackerIntId1: String,
                                        ConversionTrackerIntId2: String,
                                        AttributedEventLogFileId: String,
                                        AttributedEventIntId1: String,
                                        AttributedEventIntId2: String,
                                        AttributedEventLogEntryTime: java.sql.Timestamp,// is timestamp in parquet
                                        ConversionTrackerId: String,
                                        TrackingTagId: String,
                                        TDID: String,
                                        AdvertiserId: String,
                                        CampaignId: String,
                                        AdGroupId: String,
                                        MonetaryValue: Option[String],
                                        MonetaryValueCurrency: Option[String],
                                        ConversionTrackerLogEntryTime: java.sql.Timestamp,  // is timestamp in parquet.
                                        CampaignReportingColumnId: String,
                                        AttributionMethodId: String,
                                        CustomCPACount: Option[String],
                                        CustomRevenue: Option[String],
                                      )

case class DailyAttributionDataset(experimentOverride: Option[String] = None) extends KongMingDataset[DailyAttributionRecord](
  s3DatasetPath = "dailyattribution/v=1",
  experimentOverride = experimentOverride
)


final case class DailyAttributionEventsRecord(
                                               BidRequestId: String,
                                               AdvertiserId: String,
                                               CampaignId: String,
                                               AdGroupId: String,
                                               Target: Int,
                                               Revenue: Option[BigDecimal],
                                               CustomCPACount: Double,
                                               ConversionTrackerLogEntryTime: java.sql.Timestamp,
                                               AttributedEventLogEntryTime: java.sql.Timestamp,
                                       )


case class DailyAttributionEventsDataset(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[DailyAttributionEventsRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/dailyattributionevents/v=1",
    fileFormat = Parquet,
    experimentOverride = experimentOverride
  ) {
}
