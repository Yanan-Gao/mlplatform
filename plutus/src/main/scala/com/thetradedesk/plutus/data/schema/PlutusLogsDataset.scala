package com.thetradedesk.plutus.data.schema

case class PlutusLogsData(
                           Mu: Double,
                           Sigma: Double,
                           GSS: Double,
                           AlternativeStrategyPush: Double,
                           Model: String,
                           Strategy: Int,
                           BidRequestId: String,
                           SupplyVendor: String,
                           AdgroupId: String,
                           InitialBid: Double,
                           FinalBidPrice: Double,
                           Discrepancy: Double,
                           BaseBidAutoOpt: Double,
                           LegacyPcPushdown: Double,
                           OptOutDueToFloor: Boolean,
                           FloorPrice: Double,
                           PartnerSample: Boolean,
                         )

object PlutusLogsDataset {
  val S3PATH = "s3://ttd-identity/datapipeline/prod/pcresultslog/v=1/"
}