package com.thetradedesk.data.schema

final case class AdjImpressions(BidRequestId: String, i_DealId: String, MediaCostCPMInUSD: Double, RealMediaCostInUSD: Double, RealMediaCost: Double, DiscrepancyAdjustmentMultiplier: Double, i_RealBidPrice: Double, i_OrigninalBidPrice: Double, i_FirstPriceAdjustment: Double, imp_adjuster: Double)

final case class Impressions(BidRequestId: String, PartnerId: String, SupplyVendor: String, AdWidthInPixels: Int, AdHeightInPixels: Int, DealId: String, DiscrepancyAdjustmentMultiplier: BigDecimal, FirstPriceAdjustment: BigDecimal, MediaCostCPMInUSD: BigDecimal, SubmittedBidAmountInUSD: BigDecimal)

final case class EmpiricalDiscrepancy(PartnerId: String, SupplyVendor: String, DealId: String, AdFormat: String, EmpiricalDiscrepancy: BigDecimal)

object BidFeedbackDataset {

  val BFS3 = f"s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=4/"

}
