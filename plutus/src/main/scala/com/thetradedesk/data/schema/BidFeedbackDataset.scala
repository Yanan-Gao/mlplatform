package com.thetradedesk.data.schema

final case class AdjImpressions(BidRequestId: String, DealId: String, MediaCostCPMInUSD: Double, RealMediaCostInUSD: Double, RealMediaCost: Double, DiscrepancyAdjustmentMultiplier: Double, i_RealBidPrice: Float, SubmittedBidAmountInUSD: Double, FirstPriceAdjustment: Double, imp_adjuster: Double )
final case class Impressions(BidRequestId: String, PartnerId: String, SupplyVendor: String, AdWidthInPixels:Int, AdHeightInPixels: Int, DealId: String, DiscrepancyAdjustmentMultiplier: Double, FirstPriceAdjustment: Double, MediaCostCPMInUSD: Double, RealMediaCostInUSD: Double, RealMediaCost: Double)
final case class EmpiricalDiscrepancy(PartnerId: String, SupplyVendor: String, DealId: String, AdFormat: String, DiscrepancyAdjustmentMultiplier: Int, EmpiricalDiscrepancy: Float )

object BidFeedbackDataset {

  val BFS3 = f"s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=4/"

}
