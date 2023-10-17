package com.thetradedesk.kongming.datasets
import com.thetradedesk.spark.util.TTDConfig.config

final case class ImpressionForIsotonicRegRecord(
                                                Level:String,
                                                Id: String,
                                                Score: Double,
                                                Label: Int,
                                                ImpressionWeightForCalibrationModel: Double
                                              )

case class ImpressionForIsotonicRegDataset(experimentOverride: Option[String] = None) extends KongMingDataset[ImpressionForIsotonicRegRecord](
  s3DatasetPath = "impressionforisotonicregression/v=1",
  experimentOverride = experimentOverride
)