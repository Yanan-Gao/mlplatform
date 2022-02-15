package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import org.apache.commons.lang3.StringUtils.strip

object S3Roots {
  val IDENTITY_ROOT: String = "s3a://ttd-identity/datapipeline"
  val IDENTITY_SOURCES_ROOT: String = "s3a://ttd-identity/datapipeline/sources"
  val DATAPIPELINE_SOURCES: String = "s3a://ttd-datapipe-data/parquet"
  val PROVISIONING_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning"
  val QUBOLE_TTD_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/ttd"
  val QUBOLE_ADHOC_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/adhoc"
  val QUBOLE_VERTICA_ROOT: String = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/vertica"
  val VERTICA_ROOT: String = "s3a://thetradedesk-useast-hdreports/Vertica"
  val INSIGHTS_ROOT: String = "s3a://ttd-insights/datapipeline"
  val INSIGHTS_MIP_ROOT: String = "s3a://ttd-insights/graph"
  val INSIGHTS_OCL_ROOT: String = "s3a://ttd-insights/graph"
  val INSIGHTS_TTDGRAPH_ROOT: String = "s3a://ttd-insights/graph"
  val DATAMARKETPLACE_ROOT: String = "s3a://ttd-datamarketplace/datapipeline"
  val DATAMARKETPLACE_COUNTS_ROOT: String = "s3a://ttd-datamarketplace/counts"
  val FREQUENCY_MAPS_ROOT: String = "s3a://thetradedesk-useast-qubole/frequency-maps"
  val RAMV3_VALIDATION_ROOT: String = "s3a://thetradedesk-useast-qubole/ramv3-model-samples-results"
  val VNEXT_VALIDATION_ROOT: String = "s3a://thetradedesk-useast-qubole/ramvnext-model-samples-results"
  val LOGS2_ROOT: String = "s3a://thetradedesk-useast-logs-2"
  val DATA_IMPORT_ROOT: String = "s3a://thetradedesk-useast-data-import"
  val FORECASTING_ROOT: String = "s3a://ttd-identity/datapipeline/ram"
  val EcommerceData_ROOT: String = "s3a://thetradedesk-useast-data-import/ecommerce"
  val DEV_ROOT: String = "s3a://thetradedesk-dev"

  def getParquetDatapipeSource(topic: String, version: Int): String = s"$DATAPIPELINE_SOURCES/${strip(topic, "/")}/v=$version"

}
