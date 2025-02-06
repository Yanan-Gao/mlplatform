package com.thetradedesk.kongming.datasets
import com.thetradedesk.spark.datasets.core.Csv
import com.thetradedesk.spark.util.TTDConfig.config

final case class ImpressionForIsotonicRegRecord(
                                                Level:String,
                                                Id: String,
                                                Score: Array[Double],
                                                Label: Int,
                                                ImpressionWeightForCalibrationModel: Double
                                              )

case class ImpressionForIsotonicRegDataset(experimentOverride: Option[String] = None) extends KongMingDataset[ImpressionForIsotonicRegRecord](
  s3DatasetPath = "impressionforisotonicregression/v=2",
  experimentOverride = experimentOverride
)

case class SampledImpressionForIsotonicRegDataset(experimentOverride: Option[String] = None) extends KongMingDataset[OutOfSampleAttributionRecord](
  s3DatasetPath = "calibration/sampledimpressionforscoring/v=1",
  fileFormat = Csv.WithHeader,
  experimentOverride = experimentOverride
)