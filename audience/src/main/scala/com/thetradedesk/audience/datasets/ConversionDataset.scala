package com.thetradedesk.audience.datasets

final case class ConversionRecord(TrackingTagId: String,
                                  LogEntryTime: java.sql.Timestamp,
                                  AdvertiserId: String,
                                  TDID: String,
                                  BidRequestId: Option[String],
                                  OfflineConversionTime: Option[java.sql.Timestamp]//,
                                  //UnifiedId2: String //apparently dataset does not have this. will need to reconsider.
                                 )

case class ConversionDataset() extends
  LightReadableDataset[ConversionRecord]("/parquet/rtb_conversiontracker_verticaload/v=4", "s3a://ttd-datapipe-data")
