package com.ttd.ml.util.elDoradoUtilities.datasets.sources

import com.ttd.ml.util.elDoradoUtilities.datasets.core.DataPipeS3DataSet
import com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb.conversiontracker.ConversionTrackerRecordV4

case class ConversionTrackerDataSetV4() extends DataPipeS3DataSet[ConversionTrackerRecordV4](
  "rtb_conversiontracker_cleanfile/v=4",
  timestampFieldName = "LogEntryTime",
  mergeSchema = true
)