package com.thetradedesk.kongming.datasets
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
                                              AttributedEventTypeId: String
                                      )
object AttributedEventResultDataset {
  val S3Path = "s3a://ttd-identity/datapipeline/sources/firstpartydata_v2/attributedeventresult/"
  // date = xxxx-xx-xx
}