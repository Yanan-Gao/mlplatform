package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

case class PredictiveClearingModeLookupRecord(value: Int = 0)

object PredictiveClearingModeLookupRecord {
  val Disabled = 0
  val AdjustmentNotFound = 1
  val WithFeeUsingOriginalBid = 3
}