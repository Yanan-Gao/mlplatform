package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.Csv
import com.thetradedesk.spark.util.TTDConfig.config

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}

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
  ) {
  override protected def getMetastoreTableName: String = "sampledimpressionforscoring"
  override protected def isMetastoreCompatibleDataFormat: Boolean = false
}

case class ArraySampledImpressionForIsotonicRegDataset(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3CBufferDataset[ArrayOutOfSampleAttributionRecord](
    MLPlatformS3Root, s"${BaseFolderPath}/calibration/sampledimpressionforscoring/v=2", experimentOverride = experimentOverride)
