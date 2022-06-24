package com.thetradedesk.kongming.datasets

final case class AdGroupCvrForBiasTuningRecord(
                                                 AdGroupIdStr: String,
                                                 CVR: Double,
                                                 AdGroupId: Int
                                              )

object AdGroupCvrForBiasTuningDataset extends KongMingDataset[AdGroupCvrForBiasTuningRecord](
  s3DatasetPath = "adgroupcvrforbiastuning/v=1",
  defaultNumPartitions = 1
)