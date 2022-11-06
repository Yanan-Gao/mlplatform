package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

final case class TargetingDataRecord(TargetingDataId: BigInt,
                                    DataOwnerId: String,
                                    DataOwnerTypeId: Int,
                                    DataTypeId: Int,
                                    DataName: String,
                                    RecencyIsExternallyManaged: Boolean,
                                    UsedForFeatureTargeting: Boolean,
                                    ImportanceRank: Int,
                                    DoNotUpdateUniques: Boolean,
                                    AlwaysShowButIgnoreUniques: Boolean,
                                    isLegacyTrackingTag: Boolean,
                                    CreativeId: String,
                                    SupplyVendorId: BigInt,
                                    TestImportanceRank: Int,
                                    CreatedAt: java.sql.Timestamp
                                    )

case class TargetingDataDataset() extends
  ProvisioningS3DataSet[TargetingDataRecord]("targetingdata/v=1", true)
