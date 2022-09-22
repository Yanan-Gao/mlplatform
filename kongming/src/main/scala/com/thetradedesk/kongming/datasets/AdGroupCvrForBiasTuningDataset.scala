package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.Csv

final case class AdGroupCvrForBiasTuningRecord(
                                                 AdGroupIdStr: String,
                                                 CVR: Double,
                                                 AdGroupId: Int
                                              )

case class AdGroupCvrForBiasTuningDataset() extends KongMingDataset[AdGroupCvrForBiasTuningRecord](
  s3DatasetPath = "adgroupcvrforbiastuning/v=1",
  fileFormat = Csv.WithHeader
)