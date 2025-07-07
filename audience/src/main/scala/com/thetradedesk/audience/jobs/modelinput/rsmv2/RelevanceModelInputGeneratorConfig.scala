package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.datasets.Model
import com.thetradedesk.audience.{audienceVersionDateFormat, dateTime, ttdEnv, ttdWriteEnv}
import com.thetradedesk.spark.util.TTDConfig.config

import java.time.format.DateTimeFormatter

case class RelevanceModelInputGeneratorJobConfig(
  modelName: String,
  useTmpFeatureGenerator: Boolean,
  extraSamplingThreshold: Double,
  rsmV2FeatureSourcePath: String,
  rsmV2FeatureDestPath: String,
  subFolder: String,
  optInSeedEmptyTagPath: String,
  densityFeatureReadPathWithoutSlash: String,
  sensitiveFeatureColumn: String,
  persistHoldoutSet: Boolean,
  optInSeedType: String,
  optInSeedFilterExpr: String,
  posNegRatio: Int,
  lowerLimitPosCntPerSeed: Int,
  RSMV2UserSampleSalt: String,
  RSMV2PopulationUserSampleIndex: Seq[Int],
  RSMV2UserSampleRatio: Int,
  samplerName: String,
  overrideMode: Boolean,
  splitRemainderHashSalt: String,
  upLimitPosCntPerSeed: Int,
  saveIntermediateResult: Boolean,
  intermediateResultBasePathEndWithoutSlash: String,
  maxLabelLengthPerRow: Int,
  minRowNumsPerPartition: Int,
  trainValHoldoutTotalSplits: Int,
  activeSeedIdWhiteList: String,
  date_time: String
)

object RelevanceModelInputGeneratorConfig {
  var model: Model = Model.RSMV2
  var useTmpFeatureGenerator: Boolean = false
  var extraSamplingThreshold: Double = 0.05
  var rsmV2FeatureSourcePath: String = "/featuresV2.json"
  var rsmV2FeatureDestPath: String = ""
  var subFolder: String = "Full"
  var optInSeedEmptyTagPath: String = ""
  var densityFeatureReadPathWithoutSlash: String = "profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1"
  var sensitiveFeatureColumns: Array[String] = Array.empty[String]
  var persistHoldoutSet: Boolean = true
  var optInSeedType: String = "Active"
  var optInSeedFilterExpr: String = "true"
  var posNegRatio: Int = 50
  var lowerLimitPosCntPerSeed: Int = 200
  var RSMV2UserSampleSalt: String = "TRM"
  var RSMV2PopulationUserSampleIndex: Seq[Int] = Seq(3,5,7)
  var RSMV2UserSampleRatio: Int = 1
  var samplerName: String = "RSMV2"
  var overrideMode: Boolean = false
  var splitRemainderHashSalt: String = "split_RSMV2"
  var upLimitPosCntPerSeed: Int = 40000
  var saveIntermediateResult: Boolean = false
  var intermediateResultBasePathEndWithoutSlash: String = ""
  var maxLabelLengthPerRow: Int = 50
  var minRowNumsPerPartition: Int = 100000
  var trainValHoldoutTotalSplits: Int = 10

  var activeSeedIdWhiteList: String = ""

  def update(conf: RelevanceModelInputGeneratorJobConfig): Unit = {
    model = Model.withName(conf.modelName)
    useTmpFeatureGenerator = conf.useTmpFeatureGenerator
    extraSamplingThreshold = conf.extraSamplingThreshold
    rsmV2FeatureSourcePath = conf.rsmV2FeatureSourcePath
    rsmV2FeatureDestPath = conf.rsmV2FeatureDestPath
    subFolder = conf.subFolder
    optInSeedEmptyTagPath = conf.optInSeedEmptyTagPath
    densityFeatureReadPathWithoutSlash = conf.densityFeatureReadPathWithoutSlash
    sensitiveFeatureColumns = conf.sensitiveFeatureColumn.split(",").map(_.trim).filter(_.nonEmpty)
    persistHoldoutSet = conf.persistHoldoutSet
    optInSeedType = conf.optInSeedType
    optInSeedFilterExpr = conf.optInSeedFilterExpr
    posNegRatio = conf.posNegRatio
    lowerLimitPosCntPerSeed = conf.lowerLimitPosCntPerSeed
    RSMV2UserSampleSalt = conf.RSMV2UserSampleSalt
    RSMV2PopulationUserSampleIndex = conf.RSMV2PopulationUserSampleIndex
    RSMV2UserSampleRatio = conf.RSMV2UserSampleRatio
    samplerName = conf.samplerName
    overrideMode = conf.overrideMode
    splitRemainderHashSalt = conf.splitRemainderHashSalt
    upLimitPosCntPerSeed = conf.upLimitPosCntPerSeed
    saveIntermediateResult = conf.saveIntermediateResult
    intermediateResultBasePathEndWithoutSlash = conf.intermediateResultBasePathEndWithoutSlash
    maxLabelLengthPerRow = conf.maxLabelLengthPerRow
    minRowNumsPerPartition = conf.minRowNumsPerPartition
    trainValHoldoutTotalSplits = conf.trainValHoldoutTotalSplits
    activeSeedIdWhiteList = conf.activeSeedIdWhiteList
  }
}