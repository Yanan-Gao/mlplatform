package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.datasets.Model
import com.thetradedesk.audience.{audienceVersionDateFormat, dateTime, ttdEnv}
import com.thetradedesk.spark.util.TTDConfig.config

import java.time.format.DateTimeFormatter

object RelevanceModelInputGeneratorConfig {
  val model = Model.withName(config.getString("modelName", default = "RSMV2"))
  val useTmpFeatureGenerator = config.getBoolean("useTmpFeatureGenerator", default = false)
  val extraSamplingThreshold = config.getDouble("extraSamplingThreshold", 0.05)
  val rsmV2FeatureSourcePath = config.getString("rsmV2FeatureSourcePath", "/featuresV2.json")
  val rsmV2FeatureDestPath = config.getString("rsmV2FeatureSourcePath", s"s3a://thetradedesk-mlplatform-us-east-1/configdata/${ttdEnv}/audience/schema/RSMV2/v=1/${dateTime.format(DateTimeFormatter.ofPattern(audienceVersionDateFormat))}/features.json")
  val subFolder = config.getString("subFolder", "Full")
  val optInSeedEmptyTagPath = config.getString("optInSeedEmptyTagPath", s"s3a://thetradedesk-mlplatform-us-east-1/data/${ttdEnv}/audience/RSMV2/Seed_None/v=1/${dateTime.format(DateTimeFormatter.ofPattern(audienceVersionDateFormat))}/_${subFolder}_EMPTY")
  val persistHoldoutSet = config.getBoolean("persistHoldoutSet", true)
  val optInSeedType = config.getString("optInSeedType", "Active")
  val optInSeedFilterExpr = config.getString("optInSeedFilterExpr", "true")
  val posNegRatio = config.getInt("posNegRatio", 50)
  val lowerLimitPosCntPerSeed = config.getInt("lowerLimitPosCntPerSeed", 200)
  // this value have to be aligned with feature store side
  val RSMV2UserSampleSalt = config.getString(s"RSMV2UserSampleSalt", default = "TRM")
  val RSMV2UserSampleRatio = config.getInt("RSMV2UserSampleRatio", 1) // range 1-10
  val samplerName = config.getString("samplerName", "RSMV2")
  val overrideMode = config.getBoolean("overrideMode", false)
  val splitRemainderHashSalt = config.getString("splitRemainderHashSalt", "split_RSMV2")
  val upLimitPosCntPerSeed = config.getInt("upLimitPosCntPerSeed", 40000)
  val saveIntermediateResult = config.getBoolean("saveIntermediateResult", false)
  val intermediateResultBasePathEndWithoutSlash = config.getString("intermediateResultBasePathEndWithoutSlash", s"thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/allinone/dataset/${subFolder}")
  val maxLabelLengthPerRow = config.getInt("maxLabelLengthPerRow", 50)
  val minRowNumsPerPartition = config.getInt("minRowNumsPerPartition", 100000)
  val trainValHoldoutTotalSplits = config.getInt("trainValHoldoutTotalSplits", 10)

  val activeSeedIdWhiteList = config.getString("activeSeedIdWhiteList", "")
}