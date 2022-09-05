package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class TrackingTagRecord(AdvertiserId: String,
                                   TrackingTagId: String,
                                   TrackingTagName: String,
                                   TrackingTagTypeId: String,
                                   TagLocation: Option[String],
                                   Revenue: Option[String],
                                   Currency: Option[String],
                                   ContainerTagBody: Option[String],
                                   CreatedAt: java.sql.Timestamp,
                                   LastUpdatedAt: java.sql.Timestamp,
                                   IsVisible: Boolean,
                                   TagRedirectUrl: Option[String],
                                   TargetingDataId: BigInt,
                                   HouseholdEnabled: Boolean,
                                   HouseholdTargetingDataId: Option[BigInt],
                                   ModelingEnabled: Option[Boolean],
                                   DailyConversionLimit: Option[BigInt],
                                   OfflineDataProviderId: Option[String]
                                 )

case class TrackingTagDataset() extends
  ProvisioningS3DataSet[TrackingTagRecord]("trackingtag/v=1", true)
