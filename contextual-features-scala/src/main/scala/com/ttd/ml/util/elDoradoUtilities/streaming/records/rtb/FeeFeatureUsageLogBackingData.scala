package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb

case class FeeFeatureUsageLogBackingData(FeeFeatureType: FeeFeatureLookupRecord, FeeAmount: BigDecimal, MarginType: FeeFeatureMarginTypeLookupRecord, PassThroughFeeCardId: Option[Long], PassThroughFeeId: Option[Long], IsMargin: Boolean)
