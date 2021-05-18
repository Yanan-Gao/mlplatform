package com.thetradedesk.data.schema

final case class Svb(RequestName: String , SupplyVendor: String, SupplyVendorId: String , DiscrepancyAdjustment: Double)
final case class Pda(SupplyVendorName: String , SupplyVendor: String , PartnerId: String, DiscrepancyAdjustment: Double)
final case class Deals(SupplyVendor: String, SupplyVendorDealCode: String, IsVariablePrice: Boolean)

object DiscrepancyDataset {

  val SBVS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendorbidding/v=1/"
  val PDAS3 = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/partnersupplyvendordiscrepancyadjustment/v=1/"
  val DEALSS3 = f"s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendordeal/v=1/"

}
