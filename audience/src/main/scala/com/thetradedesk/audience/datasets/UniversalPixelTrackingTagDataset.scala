package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class UniversalPixelTrackingTagRecord(UPixelId: String,
                                   TrackingTagId: String,
                                   UrlPattern: String,
                                   SortOrder: Int,
                                   DeletionDate: java.sql.Timestamp,
                                   IsExact: Boolean,
                                   DescriptionId: Int
                                 )

case class UniversalPixelTrackingTagDataset() extends
  ProvisioningS3DataSet[UniversalPixelTrackingTagRecord]("universalpixeltrackingtag/v=1", true)
