package com.thetradedesk.kongming.datasets

final case class ConversionRecord(TrackingTagId: String,
                                  LogEntryTime: java.sql.Timestamp,
                                  AdvertiserId: String,
                                  TDID: String,
                                  BidRequestId: Option[String],
                                  OfflineConversionTime: Option[java.sql.Timestamp]//,
                                  //UnifiedId2: String //apparently dataset does not have this. will need to reconsider.
                                 )

object ConversionDataset {
  val S3Path: String = f"s3a://ttd-datapipe-data/parquet/rtb_conversiontracker_verticaload/v=4/"
}
