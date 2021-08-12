package com.ttd.ml.util.elDoradoUtilities.streaming.records.rtb

case class FeeFeatureAdjustmentContext(AdjustmentOperationType: FeeFeatureAdjustmentOperationTypeLookupRecord, SamplingRateAttribute: Option[BigDecimal], FirstPriceAdjustmentAttribute: Option[BigDecimal], PredictiveClearingModeAttribute: Option[PredictiveClearingModeLookupRecord] = None, SubmittedBidInUSDAttribute: Option[BigDecimal] = None)
