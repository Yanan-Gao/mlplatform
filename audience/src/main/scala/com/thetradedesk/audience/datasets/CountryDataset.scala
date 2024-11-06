package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class CountryRecord(ShortName: String,
                         LongName: String,
                        )

case class CountryDataset() extends
  ProvisioningS3DataSet[CountryRecord]("country/v=1", true)