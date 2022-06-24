package com.thetradedesk.kongming.datasets

final case class ClickTrackerRecord(
                               BidRequestId: String,
                               ClickRedirectId: String // is ClickTrackerId
                             )

object ClickTrackerDataset {
  val S3Path = "s3a://ttd-datapipe-data/parquet/rtb_clicktracker_cleanfile/v=5/"
}