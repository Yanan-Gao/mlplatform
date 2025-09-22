package com.thetradedesk.audience.datasets


import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT
import com.thetradedesk.audience.audienceVersionDateFormat
import java.sql.Timestamp

case class AudienceTargetingDataRecord(
                          TargetingDataId: BigInt,
                          DataSource: String,
                          )
case class AudienceTargetingDataSet() 
  extends LightReadableDataset[AudienceTargetingDataRecord](
    "audiencetargetingdata/v=1/",
    PROVISIONING_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )

object SegmentDataSource extends Enumeration {
  type Tag = Value
  val None, Tpd, Fpd, ExtendedFpd = Value
}