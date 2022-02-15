package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

case class FeeFeatureMarginTypeLookupRecord(value: Int = 0)

object FeeFeatureMarginTypeLookupRecordEnum extends Enumeration {
  val None = Value(0)
  val NonMargin = Value(1)
  val Margin = Value(2)
  val Passthrough = Value(3)
  val CurrencyBuffer = Value(4)
}
