package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class UniversalPixelRecord(UPixelId: String,
                                      AdvertiserId: String,
                                      Name: String,
                                      DeletionDate: java.sql.Timestamp,
                                      TrackedAppVendorid: Option[Int],
                                      UseTrackedAppVendorAttribution: Boolean,
                                      TrackedAppOSFamilyId: Option[Int]
                                     )

case class UniversalPixelDataset() extends
  ProvisioningS3DataSet[UniversalPixelRecord]("universalpixel/v=1", true)