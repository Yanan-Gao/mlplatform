package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class AdvertiserRecord(AdvertiserId: String,
                                  AttributionClickLookbackWindowInSeconds: Int,
                                  AttributionImpressionLookbackWindowInSeconds: Int
                               )

case class AdvertiserDataSet() extends ProvisioningS3DataSet[AdvertiserRecord]("advertiser/v=1"){}
