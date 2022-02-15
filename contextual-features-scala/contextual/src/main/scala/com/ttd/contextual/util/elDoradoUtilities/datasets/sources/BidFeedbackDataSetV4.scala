package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.DataPipeS3DataSet
import com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb.bidfeedback.BidFeedbackRecordV4

case class BidFeedbackDataSetV4()
  extends DataPipeS3DataSet[BidFeedbackRecordV4](
    "rtb_bidfeedback_cleanfile/v=4",
    timestampFieldName = "LogEntryTime",
    // this needs to be disabled for large tables, the performance hit is too big
    mergeSchema = false
  ) {
}
