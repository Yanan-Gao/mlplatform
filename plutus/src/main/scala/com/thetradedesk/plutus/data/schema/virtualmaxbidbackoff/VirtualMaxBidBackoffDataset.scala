package com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff;

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset;

object VirtualMaxBidBackoffDataset extends S3DailyParquetDataset[VirtualMaxBidBackoffSchema]{
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String =
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/propellerbackoff/v=${DATA_VERSION}"
}


case class VirtualMaxBidBackoffSchema(
  CampaignId: String,
  UnderdeliveryFraction: Double,
  UnderdeliveryFraction_History: Array[Double],
  VirtualMaxBid_Quantile: Int,
  VirtualMaxBid_Quantile_History: Array[Int],
  BidsCloseToMaxBid_Fraction: Double,
  BidsCloseToMaxBid_Fraction_History: Array[Double],
  SumInternalBidOverMaxBid_Fraction: Double,
  SumInternalBidOverMaxBid_Fraction_History: Array[Double],

  VirtualMaxBid_Multiplier: Double,
  VirtualMaxBid_Multiplier_Uncapped: Double,
  VirtualMaxBid_Multiplier_Options: Array[Double],
)