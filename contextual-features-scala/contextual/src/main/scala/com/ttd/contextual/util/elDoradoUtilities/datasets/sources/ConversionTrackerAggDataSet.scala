package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{ColumnExistsInDataSet, DateHourFormatStrings, DateHourPartitionedS3DataSet, DefaultTimeFormatStrings, S3Roots, SourceDataSet}

case class ConversionTrackerAggDataSet() extends DateHourPartitionedS3DataSet[ConversionTrackerAggDataRecord](
  SourceDataSet,
  S3Roots.IDENTITY_ROOT,
  "conversiontrackeraggregated/v=1",
  dateField = "date" -> ColumnExistsInDataSet,
  hourField = "hour" -> ColumnExistsInDataSet,
  formatStrings = DateHourFormatStrings(
    DefaultTimeFormatStrings.dateTimeFormatString,
    "H"
  )
)

case class ConversionTrackerAggDataRecord(
                                     tdid1: Long,
                                     tdid2: Long,
                                     TrackingTagId: String,
                                     Advertiserid: String,
                                     AllowTargeting: Boolean
                                   )

