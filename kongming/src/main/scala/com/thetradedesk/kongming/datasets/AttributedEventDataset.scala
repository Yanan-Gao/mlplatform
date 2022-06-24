package com.thetradedesk.kongming.datasets
import java.sql.Timestamp

final case class AttributedEventRecord(
                                        AttributedEventId: String,
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
                                        AdGroupId: String
                                      )
object AttributedEventDataset {
  val S3Path = "s3a://ttd-identity/datapipeline/sources/firstpartydata_v2/attributedevent/"
  // date = xxxx-xx-xx
}