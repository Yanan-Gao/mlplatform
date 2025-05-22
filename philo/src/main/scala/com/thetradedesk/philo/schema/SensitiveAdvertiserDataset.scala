package com.thetradedesk.philo.schema

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
case class SensitiveAdvertiserRecord(AdvertiserId: String, CategoryPolicy: String, IsRestricted: Integer)

case class SensitiveAdvertiserDataset() extends ProvisioningS3DataSet[SensitiveAdvertiserRecord]("distributedalgosadvertiserrestrictionstatus/v=1", true) {}
