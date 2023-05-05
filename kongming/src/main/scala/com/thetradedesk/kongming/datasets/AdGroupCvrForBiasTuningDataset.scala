package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

final case class AdGroupCvrForBiasTuningRecord(
                                                 AdGroupIdStr: String,
                                                 CVR: Double,
                                                 AdGroupId: Int
                                              )

case class AdGroupCvrForBiasTuningDataset(experimentName: String = "") extends KongMingDataset[AdGroupCvrForBiasTuningRecord](
  s3DatasetPath = "adgroupcvrforbiastuning/v=1",
  experimentName = config.getString("ttd.AdGroupCvrForBiasTuningDataset.experimentName", experimentName)
)