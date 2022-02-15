package com.ttd.contextual.util.elDoradoUtilities.datasets.sources

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.DataPipeS3DataSet
import com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb.bidrequest.BidRequestRecordV4

case class BidRequestDataSetV4()
  extends DataPipeS3DataSet[BidRequestRecordV4](
    "rtb_bidrequest_cleanfile/v=4",
    timestampFieldName = "LogEntryTime",
    // this needs to be disabled for large tables, the performance hit is too big
    mergeSchema = false
  ) {
}