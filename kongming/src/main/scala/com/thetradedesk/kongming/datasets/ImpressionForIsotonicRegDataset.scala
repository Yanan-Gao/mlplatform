package com.thetradedesk.kongming.datasets
import com.thetradedesk.spark.util.TTDConfig.config

final case class ImpressionForIsotonicRegRecord(
                                                AdGroupIdStr: String,
                                                AdGroupId: Int,
                                                Score: Double,
                                                Label: Int,
                                                ImpressionWeightForCalibrationModel: Double
                                              )

case class ImpressionForIsotonicRegDataset(experimentName: String = "") extends KongMingDataset[ImpressionForIsotonicRegRecord](
  s3DatasetPath = "impressionforisotonicregression/v=1",
  experimentName = config.getString("ttd.ImpressionForIsotonicRegDataset.experimentName", experimentName)
)