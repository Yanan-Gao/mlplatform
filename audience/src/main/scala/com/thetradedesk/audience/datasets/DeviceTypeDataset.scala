package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv, getClassName, audienceVersionDateFormat}
import com.thetradedesk.spark.util.TTDConfig.config

final case class DeviceTypeRecord(
                         DeviceTypeId: Int,
                         DeviceTypeName: String
                       )

case class DeviceTypeDataset() extends LightReadableDataset[DeviceTypeRecord]("devicetype/v=1", PROVISIONING_ROOT, dateFormat = audienceVersionDateFormat.substring(0, 8))