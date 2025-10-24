package com.thetradedesk.audience.datasets


import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT
import com.thetradedesk.audience.audienceVersionDateFormat
import java.sql.Timestamp

case class AdvertiserRecord(
                          AdvertiserId: String,
                          IndustryCategoryId: BigInt,
                          CurrencyCodeId: String
                          )
case class AdvertiserDataSet() 
  extends LightReadableDataset[AdvertiserRecord](
    "advertiser/v=1/",
    PROVISIONING_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )