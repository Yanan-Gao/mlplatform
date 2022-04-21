package com.thetradedesk.kongming.datasets

final case class AdvertiserRecord(AdvertiserId: String,
                                  AttributionClickLookbackWindowInSeconds: Int,
                                  AttributionImpressionLookbackWindowInSeconds: Int
                               )

object AdvertiserDataset {
  val S3Path = "s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/advertiser/v=1/"
}