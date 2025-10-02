package com.thetradedesk.frequency.schema

case class ClickTrackerRecord(BidRequestId: String)

object ClickTrackerDataSet {
  // Base path; jobs will append date=YYYYMMDD at read time
  val CLICKSS3: String = "s3://ttd-datapipe-data/parquet/rtb_clicktracker_verticaload/v=1/"
}