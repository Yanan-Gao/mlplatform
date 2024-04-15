package com.thetradedesk.audience.configs

import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.audience.datasets.{DataSource, Model}
import com.thetradedesk.audience.utils.S3Utils;

object AudienceModelInputGeneratorConfig {
  // ********************* used for generator job *********************
  // val model = Model.withName(config.getStringRequired("modelName"))

  // val supportedDataSources = config.getStringRequired("supportedDataSources").split(',')
  //   .map(dataSource => DataSource.withName(dataSource).id)

  // val saltToSplitDataset = config.getStringRequired("saltToSplitDataset")

  val model = Model.withName(config.getString("modelName", default = "RSM"))

  val supportedDataSources = config.getString("supportedDataSources", default = "Seed").split(',')
    .map(dataSource => DataSource.withName(dataSource).id)

  val saltToSplitDataset = config.getString("saltToSplitDataset", default = "RSMSplit")

  val validateDatasetSplitModule = config.getInt("validateDatasetSplitModule", default = 5)

  var subFolder = config.getString("subFolder", "split")

  var persistHoldoutSet = config.getBoolean("persistHoldoutSet", default = false)

  var seedSizeLowerScaleThreshold = config.getInt("seedSizeLowerScaleThreshold", default = 1)

  var seedSizeUpperScaleThreshold = config.getInt("seedSizeUpperScaleThreshold", default = 12)

  // ***************** used for model input generator *********************
  val numTDID = config.getInt("numTDID", 100)

  val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1)

  val seenInBiddingLookBack = config.getInt("seenInBiddingLookBack", 1)

  // detect recent seed raw data path in airflow and pass to spark job
  val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", "None")
  val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
  val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1/SeedId="))

  // n bid impressions we care about
  val lastTouchNumberInBR = config.getInt("lastTouchNumberInBR", 3)

  // conversion data look back days
  val conversionLookBack = config.getInt("conversionLookBack", 1)

  // default value of minimal positive label size in SIB dataset
  val minimalPositiveLabelSizeOfSIB = config.getInt("minimalPositiveLabelSizeOfSIB", 0)

  // todo merge threshold settings into policy table
  val positiveSampleUpperThreshold = config.getDouble("positiveSampleUpperThreshold", default = 20000.0)

  val positiveSampleLowerThreshold = config.getDouble("positiveSampleLowerThreshold", default = 2000.0)

  val positiveSampleSmoothingFactor = config.getDouble("positiveSampleSmoothingFactor", default = 0.95)

  val negativeSampleRatio = config.getInt("negativeSampleRatio", default = 5)

  val labelMaxLength = config.getInt("labelMaxLength", default = 50)

  val recordIntermediateResult = config.getBoolean("recordIntermediateResult", default = false)

  val bidImpressionRepartitionNumAfterFilter = config.getInt("bidImpressionRepartitionNumAfterFilter", 8192)

  // the way to determine the n tdid selection->
  // 0: last n tdid; 1: even stepwise selection for n tdid; 2 and other: random select n tdid
  val tdidTouchSelection = config.getInt("tdidTouchSelection", default = 1)
}
