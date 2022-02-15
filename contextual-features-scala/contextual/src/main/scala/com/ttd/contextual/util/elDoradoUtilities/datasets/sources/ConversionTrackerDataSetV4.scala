package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.DataPipeS3DataSet
import com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb.conversiontracker.ConversionTrackerRecordV4

case class ConversionTrackerDataSetV4() extends DataPipeS3DataSet[ConversionTrackerRecordV4](
  "rtb_conversiontracker_cleanfile/v=4",
  timestampFieldName = "LogEntryTime",
  mergeSchema = true
)