package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.audienceVersionDateFormat
import com.thetradedesk.audience.datasets.S3Roots.PROVISIONING_ROOT

case class SupplyVendorRecord(
                               SupplyVendorId: BigInt,
                               SupplyVendorName: String
                             )
case class SupplyVendorDataSet()
  extends LightReadableDataset[SupplyVendorRecord](
    "supplyvendor/v=1",
    PROVISIONING_ROOT,
    dateFormat = audienceVersionDateFormat.substring(0, 8) // Extract the first 8 characters
  )