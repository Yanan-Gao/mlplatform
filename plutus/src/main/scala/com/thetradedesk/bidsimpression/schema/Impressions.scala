package com.thetradedesk.bidsimpression.schema

object ImpressionsCols {
  val IMPRESSIONCOLUMNS: Seq[String] = Seq(
    "BidRequestId",

    "MediaCostCPMInUSD",
    "DiscrepancyAdjustmentMultiplier",

    "FirstPriceAdjustment",
    "SubmittedBidAmountInUSD")
}