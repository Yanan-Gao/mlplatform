package com.thetradedesk.featurestore.datasets

import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet

final case class AttributedEventRecord(
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
                                        MonetaryValueCurrency: Option[String]
                                      )

case class AttributedEventDataSet() extends IdentitySourcesS3DataSet[AttributedEventRecord]("firstpartydata_v2/attributedevent")
