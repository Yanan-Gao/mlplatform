package com.thetradedesk.kongming.datasets
import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet

import java.sql.Timestamp

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
                                              CustomCPACount: Option[String],
                                              CustomRevenue: Option[String],
//                                              AttributedEventTypeId: String
                                      )

case class AttributedEventResultDataSet() extends IdentitySourcesS3DataSet[AttributedEventResultRecord]("firstpartydata_v2/attributedeventresult")
