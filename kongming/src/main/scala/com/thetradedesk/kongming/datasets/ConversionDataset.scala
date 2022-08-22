package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.{CoalesceOnWrite, DataPipeS3DataSet}
import com.thetradedesk.spark.util.CloudProvider
import com.thetradedesk.streaming.records.rtb.conversiontracker.ConversionTrackerVerticaLoadRecord

//final case class ConversionRecord(TrackingTagId: String,
//                                  LogEntryTime: java.sql.Timestamp,
//                                  AdvertiserId: String,
//                                  TDID: String,
//                                  BidRequestId: Option[String],
//                                  OfflineConversionTime: Option[java.sql.Timestamp]//,
//                                  //UnifiedId2: String //apparently dataset does not have this. will need to reconsider.
//                                 )
//
//object ConversionDataset {
//  val S3Path: String = f"s3a://ttd-datapipe-data/parquet/rtb_conversiontracker_verticaload/v=4/"
//}

case class ConversionTrackerVerticaLoadDataSetV4(cloudProvider: CloudProvider)
  extends DataPipeS3DataSet[ConversionTrackerVerticaLoadRecord](
    "rtb_conversiontracker_verticaload/v=4",
    timestampFieldName = "LogEntryTime",
    cloudProvider = cloudProvider,
    mergeSchema = false,
    partitioningType = CoalesceOnWrite
  ) {
  override protected val forceSchemaForParquet: Boolean = true
}
