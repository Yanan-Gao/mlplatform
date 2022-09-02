package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.{CoalesceOnWrite, DataPipeS3DataSet}
import com.thetradedesk.spark.util.CloudProvider
import com.thetradedesk.streaming.records.rtb.clicktracker.ClickTrackerRecord

case class ClickTrackerDataSetV5(cloudProvider: CloudProvider)
  extends DataPipeS3DataSet[ClickTrackerRecord](
    "rtb_clicktracker_cleanfile/v=5",
    timestampFieldName = "LogEntryTime",
    cloudProvider = cloudProvider,
    mergeSchema = false,
    partitioningType = CoalesceOnWrite
  ) {
  override protected val forceSchemaForParquet: Boolean = true
}
