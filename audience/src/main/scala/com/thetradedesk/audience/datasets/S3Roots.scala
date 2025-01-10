package com.thetradedesk.audience.datasets

object S3Roots {
  val IDENTITY_ROOT: String = "s3a://ttd-identity/datapipeline"
  val IDENTITY_SOURCES_ROOT: String = "s3a://ttd-identity/datapipeline/sources"
  val DATAPIPELINE_SOURCES_ROOT: String = "s3a://ttd-datapipe-data/parquet"
  val PROVISIONING_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning"
  val QUBOLE_TTD_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/ttd"
  val QUBOLE_ADHOC_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/adhoc"
  val QUBOLE_VERTICA_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/vertica"
  val ML_PLATFORM_ROOT: String = "s3a://thetradedesk-mlplatform-us-east-1/data"
  val FEATURE_STORE_ROOT: String = "s3a://thetradedesk-mlplatform-us-east-1/features/feature_store"
  val LOGS_ROOT: String = "s3a://thetradedesk-useast-logs-2"
}