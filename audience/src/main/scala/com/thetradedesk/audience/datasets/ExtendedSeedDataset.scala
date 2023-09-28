package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.audience.{seedCoalesceAfterFilter, ttdEnv}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

final case class ExtendedSeedRecord(TDID: String
                                    , tag: Long,
                                    personId: String,
                                    householdId: String)

case class ExtendedSeedReadableDataset(seedId: String) extends
  LightReadableDataset[ExtendedSeedRecord](s"${ttdEnv}/audience/seedData/${seedId}/v=1", ML_PLATFORM_ROOT)

case class ExtendedSeedWritableDataset(seedId: String) extends
  LightWritableDataset[ExtendedSeedRecord](s"${ttdEnv}/audience/seedData/${seedId}/v=1", ML_PLATFORM_ROOT, seedCoalesceAfterFilter)

object SeedTagOperations {
  // check if the data is from the cross device vendor source
  // return 1 or 0 correspondingly
  def dataSourceCheck(tag: Column, crossDeviceVendor: CrossDeviceVendor): Column = {
    shiftright(tag, crossDeviceVendor.id) bitwiseAND lit(1)
  }

  def dataSourceTag(crossDeviceVendor: CrossDeviceVendor): Column = {
    lit(1 << crossDeviceVendor.id)
  }
}