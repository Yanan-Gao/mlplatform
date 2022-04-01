package com.thetradedesk.geronimo.shared.schemas

final case class AdjImpressions(BidRequestId: String, i_DealId: String, MediaCostCPMInUSD: Double, RealMediaCostInUSD: Double, RealMediaCost: Double, DiscrepancyAdjustmentMultiplier: Double, i_RealBidPrice: Double, ImpressionsOriginalBidPrice: Double, ImpressionsFirstPriceAdjustment: Double, imp_adjuster: Double)

// note if you add fields to Impressions add them to MockData createImpressionsRow
final case class BidFeedbackRecord(BidRequestId: String, PartnerId: String, SupplyVendor: String, AdWidthInPixels: Int, AdHeightInPixels: Int, DealId: String, DiscrepancyAdjustmentMultiplier: BigDecimal, FirstPriceAdjustment: BigDecimal, MediaCostCPMInUSD: BigDecimal, SubmittedBidAmountInUSD: BigDecimal, BidFeedbackId: String, FeedbackBidderCacheMachineName: String)

final case class EmpiricalDiscrepancy(PartnerId: String, SupplyVendor: String, DealId: String, AdFormat: String, EmpiricalDiscrepancy: BigDecimal)

object BidFeedbackDataset {

  val BFS3 = f"s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=5/"

}
