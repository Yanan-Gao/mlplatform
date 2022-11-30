package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.ttdEnv
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

import com.thetradedesk.spark.datasets.core.IdentitySourcesS3DataSet
final case class CrossDeviceGraphRecord(householdID: String,
                                                  personID: String,
                                                  screenID: String,
                                                  screenIDType: Byte,
                                                  uiid: String,
                                                  uiidType: Byte,
                                                  lastSeen: Long,
                                                  score: Double,
                                                  household_score: Double,
                                                  country: String,
                                                  deviceType: Byte,
                                                  osFamily: Byte,
                                                  date: String)

case class CrossDeviceGraphDataset() extends
  LightReadableDataset[CrossDeviceGraphRecord] ("sxd-etl/universal/iav2graph", "s3://thetradedesk-useast-data-import/", source=Some(DatasetSource.CrossDeviceGraph))

final case class SampledCrossDeviceGraphRecord(
                                        personID: String,
                                        TDID: String,
                                        deviceType: Byte)
case class SampledCrossDeviceGraphDataset() extends
  LightReadableDataset[SampledCrossDeviceGraphRecord] (s"/${ttdEnv}/audience/sampledCrossDeviceGraph/v=1", S3Roots.ML_PLATFORM_ROOT)