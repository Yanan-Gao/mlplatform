package com.thetradedesk.featurestore.datasets

import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet

final case class AttributedEventResultRecord(
                                              ConversionTrackerLogFileId: String,
                                              ConversionTrackerIntId1: String,
                                              ConversionTrackerIntId2: String,
                                              AttributedEventLogFileId: String,
                                              AttributedEventIntId1: String,
                                              AttributedEventIntId2: String,
                                              ConversionTrackerLogEntryTime: String,  // is string in parquet.
                                              CampaignReportingColumnId: String,
                                              AttributionMethodId: String,
                                              //                                              AttributedEventTypeId: String
                                            )

case class AttributedEventResultDataSet() extends IdentitySourcesS3DataSet[AttributedEventResultRecord]("firstpartydata_v2/attributedeventresult")
