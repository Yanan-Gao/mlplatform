package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.Csv

final case class ImpressionForIsotonicRegRecord(
                                                AdGroupIdStr: String,
                                                AdGroupId: Int,
                                                Score: Double,
                                                Label: Int,
                                                ImpressionWeightForCalibrationModel: Double
                                              )

case class ImpressionForIsotonicRegDataset() extends KongMingDataset[ImpressionForIsotonicRegRecord](
  s3DatasetPath = "impressionforisotonicregression/v=1",
  fileFormat = Csv.WithHeader
)