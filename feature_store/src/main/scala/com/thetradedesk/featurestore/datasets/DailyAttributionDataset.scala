package com.thetradedesk.featurestore.datasets

case class DailyAttributionRecord(
                                         AttributedEventId: String,
                                         AttributedEventTypeId: String,
                                         ConversionTrackerLogFileId: String,
                                         ConversionTrackerIntId1: String,
                                         ConversionTrackerIntId2: String,
                                         AttributedEventLogFileId: String,
                                         AttributedEventIntId1: String,
                                         AttributedEventIntId2: String,
                                         AttributedEventLogEntryTime: String,// is string in parquet
                                         ConversionTrackerId: String,
                                         TrackingTagId: String,
                                         TDID: String,
                                         AdvertiserId: String,
                                         CampaignId: String,
                                         AdGroupId: String,
                                         MonetaryValue: Option[String],
                                         MonetaryValueCurrency: Option[String],
                                         ConversionTrackerLogEntryTime: String,  // is string in parquet.
                                         CampaignReportingColumnId: String,
                                         AttributionMethodId: String,
                                         Revenue: String,
                                       )


case class DailyAttributionDataset(experimentOverride: Option[String] = None) extends ProcessedDataset[DailyAttributionRecord] (
  s3DatasetPath = "dailyattribution/v=1",
  experimentOverride = experimentOverride
)
