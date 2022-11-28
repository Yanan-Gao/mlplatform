package com.thetradedesk.kongming.datasets

final case class AdGroupPolicySnapshotRecord(ConfigKey: String,
                                             ConfigValue: String,
                                             DataAggKey: String,
                                             DataAggValue: String,
                                             CrossDeviceUsage: Boolean,
                                             MinDailyConvCount: Int,
                                             MaxDailyConvCount: Int,
                                             LastTouchCount: Int,
                                             DataLookBack: Int,
                                             PositiveSamplingRate: Double,
                                             NegativeSamplingMethod: String,
                                             CrossDeviceConfidenceLevel: Double,
                                             FetchOlderTrainingData: Boolean,
                                             OldTrainingDataEpoch: Int,
                                            )

case class AdGroupPolicySnapshotDataset() extends KongMingDataset[AdGroupPolicySnapshotRecord](
  s3DatasetPath = "dailyadgrouppolicy/v=1"
)
