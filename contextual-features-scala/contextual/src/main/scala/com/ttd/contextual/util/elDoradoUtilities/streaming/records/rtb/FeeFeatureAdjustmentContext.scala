package com.ttd.contextual.util.elDoradoUtilities.streaming.records.rtb

case class FeeFeatureAdjustmentContext(AdjustmentOperationType: FeeFeatureAdjustmentOperationTypeLookupRecord, SamplingRateAttribute: Option[BigDecimal], FirstPriceAdjustmentAttribute: Option[BigDecimal], PredictiveClearingModeAttribute: Option[PredictiveClearingModeLookupRecord] = None, SubmittedBidInUSDAttribute: Option[BigDecimal] = None)
