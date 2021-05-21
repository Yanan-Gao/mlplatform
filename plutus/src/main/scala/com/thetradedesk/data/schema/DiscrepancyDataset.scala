package com.thetradedesk.data.schema

final case class Svb(RequestName: String , SupplyVendorId: String , DiscrepancyAdjustment: BigDecimal)
final case class Pda(SupplyVendorName: String , PartnerId: String, DiscrepancyAdjustment: BigDecimal)
final case class Deals(SupplyVendorId: String, SupplyVendorDealCode: String, IsVariablePrice: Boolean)

object DiscrepancyDataset {

  val SBVS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendorbidding/v=1/"
  val PDAS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/partnersupplyvendordiscrepancyadjustment/v=1/"
  val DEALSS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendordeal/v=1/"

}
