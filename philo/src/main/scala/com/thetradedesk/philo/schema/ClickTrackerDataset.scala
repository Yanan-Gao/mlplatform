package com.thetradedesk.philo.schema

case class ClickTrackerRecord(BidRequestId: String)

object ClickTrackerDataSet {
val CLICKSS3: String = "s3://ttd-datapipe-data/parquet/rtb_clicktracker_cleanfile/v=5/"
}