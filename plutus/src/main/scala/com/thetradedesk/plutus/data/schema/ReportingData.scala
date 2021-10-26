package com.thetradedesk.plutus.data.schema

case class VerticaKoaVolumeControlBudgetRecord(   AdGroupId: String,
                                                  // VolumeControlPriority: Byte,
                                                  PotentialSpendRatio: BigDecimal,
                                                  DailyAdvertiserCostInUSD: BigDecimal,
                                                  DailyAdvertiserCostInAdvertiserCurrency: BigDecimal,
                                                  DailyImpressionCount: Long,
                                                  DailyPotentialInUSD: BigDecimal,
                                                  DailyPotentialInAdvertiserCurrency: BigDecimal,
                                                  DailyPotentialInImpressions: BigDecimal,
                                                  SnapshotsWithAdGroupSpend: Long,
                                                  SnapshotsPerDay: Long,
                                                  MachineSnapshotsPerDay: Option[Long],
                                                  PotentialBidCount: Option[BigDecimal],
                                                  BidCount: Option[Long],
                                                  PacingAllowedCount: Option[Long],
                                                  FoundCount: Option[Long]
                                              )


object ReportingData {
val verticaKoaVolumeControlBudgetS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/vertica/VerticaKoaVolumeControlBudgetExportToS3/v=1/"
}
