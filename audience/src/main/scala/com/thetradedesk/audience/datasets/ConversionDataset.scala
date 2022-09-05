package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.{CoalesceOnWrite, DataPipeS3DataSet}
import com.thetradedesk.spark.util.CloudProvider
import com.thetradedesk.streaming.records.rtb.conversiontracker.ConversionTrackerVerticaLoadRecord
case class ConversionDataset(cloudProvider: CloudProvider)
  extends DataPipeS3DataSet[ConversionTrackerVerticaLoadRecord](
    "rtb_conversiontracker_verticaload/v=4",
    timestampFieldName = "LogEntryTime",
    cloudProvider = cloudProvider,
    mergeSchema = false,
    partitioningType = CoalesceOnWrite
) {
  override protected val forceSchemaForParquet: Boolean = true
}
