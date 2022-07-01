package com.thetradedesk.audience.datasets

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
  LightReadableDataset[TrackingTagRecord]("/warehouse.external/thetradedesk.db/provisioning/trackingtag/v=1", "s3a://thetradedesk-useast-qubole")
