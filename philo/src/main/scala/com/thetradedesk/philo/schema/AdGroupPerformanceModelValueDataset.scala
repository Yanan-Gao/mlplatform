package com.thetradedesk.philo.schema

case class AdGroupPerformanceModelValueRecord(AdGroupId: String, ModelType: Int, ModelVersion: Int, AlwaysApply: Boolean)

object AdGroupPerformanceModelValueDataset {
  val AGPMVS3: String = "s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/adgroupperformancemodelvalues/v=1/"
}