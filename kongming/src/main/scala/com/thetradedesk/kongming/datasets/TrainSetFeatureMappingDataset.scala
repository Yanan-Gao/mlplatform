package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core._

final case class TrainSetFeatureMappingRecord(
                                               FeatureName: String,
                                               FeatureOriginalValue: String,
                                               FeatureHashedValue: String,
                                             )
case class TrainSetFeatureMappingDataset() extends KongMingDataset[TrainSetFeatureMappingRecord](
  s3DatasetPath = s"trainsetfeaturesmapping/v=1",
  fileFormat = Parquet,
  experimentName = ""
)
