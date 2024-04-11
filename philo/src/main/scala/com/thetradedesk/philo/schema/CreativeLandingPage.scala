package com.thetradedesk.philo.schema

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
case class CreativeLandingPageRecord(CreativeId: String, CreativeLandingPageId: String)

case class CreativeLandingPageDataSet() extends ProvisioningS3DataSet[CreativeLandingPageRecord]("creativelandingpage/v=1", true) {}

