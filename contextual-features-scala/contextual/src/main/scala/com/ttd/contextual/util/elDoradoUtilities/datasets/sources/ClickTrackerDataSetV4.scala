package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.DataPipeS3DataSet
import com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb.clicktracker.ClickTrackerRecordV4

case class ClickTrackerDataSetV4() extends DataPipeS3DataSet[ClickTrackerRecordV4](
  "rtb_clicktracker_cleanfile/v=4",
  timestampFieldName = "LogEntryTime"
)
