package com.thetradedesk.kongming.datasets

final case class AdGroupCvrForBiasTuningRecord(
                                                 AdGroupIdStr: String,
                                                 CVR: Double,
                                                 AdGroupId: Int
                                              )

case class AdGroupCvrForBiasTuningDataset(experimentOverride: Option[String] = None) extends KongMingDataset[AdGroupCvrForBiasTuningRecord](
  s3DatasetPath = "adgroupcvrforbiastuning/v=1",
  experimentOverride = experimentOverride
)