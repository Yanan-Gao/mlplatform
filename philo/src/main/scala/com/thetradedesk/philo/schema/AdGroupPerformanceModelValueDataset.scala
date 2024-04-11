package com.thetradedesk.philo.schema

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
case class AdGroupPerformanceModelValueRecord(AdGroupId: String, ModelType: Int, ModelVersion: Int, AlwaysApply: Boolean)
case class AdGroupPerformanceModelValueDataSet() extends ProvisioningS3DataSet[AdGroupPerformanceModelValueRecord]("adgroupperformancemodelvalues/v=1", true) {}
