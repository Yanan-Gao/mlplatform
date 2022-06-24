package com.thetradedesk.kongming.datasets

final case class ImpressionForIsotonicRegRecord(
                                                AdGroupIdStr: String,
                                                AdGroupId: Int,
                                                PieceWeightedConversionRate: Double,
                                                Score: Double,
                                                Label: Int,
                                                ImpressionWeightForCalibrationModel: Double
                                              )

object ImpressionForIsotonicRegDataset extends KongMingDataset[ImpressionForIsotonicRegRecord](
  s3DatasetPath = "impressionforisotonicregression/v=1",
  defaultNumPartitions = 1
)