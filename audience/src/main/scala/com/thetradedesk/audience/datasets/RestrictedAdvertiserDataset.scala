package com.thetradedesk.audience.datasets


import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT
import com.thetradedesk.audience.audienceVersionDateFormat
import java.sql.Timestamp

case class RestrictedAdvertiserRecord(
                          AdvertiserId: String,
                          IsRestricted: Int
                          )

case class RestrictedAdvertiserDataSet() 
  extends LightReadableDataset[RestrictedAdvertiserRecord](
    "distributedalgosadvertiserrestrictionstatus/v=1/",
    PROVISIONING_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )
