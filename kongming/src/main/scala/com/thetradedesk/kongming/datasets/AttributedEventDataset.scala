package com.thetradedesk.kongming.datasets
import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet

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

case class AttributedEventDataSet() extends IdentitySourcesS3DataSet[AttributedEventRecord]("firstpartydata_v2/attributedevent")
