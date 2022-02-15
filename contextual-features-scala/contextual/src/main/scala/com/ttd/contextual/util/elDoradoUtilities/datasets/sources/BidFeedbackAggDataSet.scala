package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{ColumnExistsInDataSet, DateHourFormatStrings, DateHourPartitionedS3DataSet, DefaultTimeFormatStrings, S3Roots, SourceDataSet}

case class BidFeedbackAggDataSet() extends DateHourPartitionedS3DataSet[BidFeedbackAggDataRecord](
  SourceDataSet,
  S3Roots.IDENTITY_ROOT,
  "bidfeedbackaggregated/v=1",
  dateField = "date" -> ColumnExistsInDataSet,
  hourField = "hour" -> ColumnExistsInDataSet,
  formatStrings = DateHourFormatStrings(
    DefaultTimeFormatStrings.dateTimeFormatString,
    "H"
  )
)

case class BidFeedbackAggDataRecord(
                                      tdid1: Long,
                                      tdid2: Long,
                                      site: String,
                                      hits: Long,
                                      datacenters: Array[Int]
                                   )
