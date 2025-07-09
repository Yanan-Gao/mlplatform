package com.thetradedesk.audience.jobs.modelinput.rsmv2

import com.thetradedesk.audience.datasets.Model
import com.thetradedesk.audience.datasets.Model.Model

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