package com.thetradedesk.plutus.data.schema

final case class Svb(RequestName: String, SupplyVendorId: String, DiscrepancyAdjustment: BigDecimal)

final case class Pda(SupplyVendorName: String, PartnerId: String, DiscrepancyAdjustment: BigDecimal)

final case class Deals(SupplyVendorId: String, SupplyVendorDealCode: String, IsVariablePrice: Boolean)

final case class EmpiricalDiscrepancy(PartnerId: String, SupplyVendor: String, DealId: String, AdFormat: String, EmpiricalDiscrepancy: BigDecimal)

object DiscrepancyDataset {

  val SBVS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendorbidding/v=1/"
  val PDAS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/partnersupplyvendordiscrepancyadjustment/v=1/"
  val DEALSS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendordeal/v=1/"

}
