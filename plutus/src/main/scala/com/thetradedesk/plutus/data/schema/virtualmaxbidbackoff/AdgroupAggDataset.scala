package com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

case class AdgroupAggDataDataset(
                                  AdGroupId: String,
                                  SumImpressionFloor: Double,
                                  SumDealImpressionFloor: Double,
                                  Spend: Double,
                                  Bids: Long,
                                  Wins: Long,
                                  DealWins: Long,
                                  VariablePriceSpend: Double,
                                  SumFeeAmount: Double,
                                  SumInitialBid: Double,
                                  CTVSpend: Double,
                                  VariablePriceSpendPercentage: Double,
                                  CTVSpendPercentage: Double,
                                  MeanImpressionFloor: Double,
                                  MeanDealImpressionFloor: Double,
                                  IsDealOnly: Boolean,
                                  IsCTVOnly: Boolean
                                )

object AdgroupAggData extends S3DailyParquetDataset[AdgroupAggDataDataset]{
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String =
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/adgroupaggdata/v=${DATA_VERSION}"
}



