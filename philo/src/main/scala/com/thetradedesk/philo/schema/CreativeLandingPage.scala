package com.thetradedesk.philo.schema

case class CreativeLandingPageRecord(CreativeId: String, CreativeLandingPageId: String)
object CreativeLandingPageDataset {
  val CREATIVELANDINGPAGES3: String = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/creativelandingpage/v=1"
}
