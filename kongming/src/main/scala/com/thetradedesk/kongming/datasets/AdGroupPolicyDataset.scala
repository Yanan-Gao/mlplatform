package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.util.TTDConfig.config

import java.time.LocalDate

final case class AdGroupPolicyRecord(ConfigKey: String,
                                     ConfigValue: String,
                                     DataAggKey: String,
                                     DataAggValue: String,
                                     CrossDeviceUsage: Boolean,
                                     MinDailyConvCount: Int,
                                     MaxDailyConvCount: Int,
                                     LastTouchCount: Int,
                                     DataLookBack: Int,
                                     PositiveSamplingRate: Double,
                                     NegativeSamplingMethod: String,
                                     CrossDeviceConfidenceLevel: Double,
                                     FetchOlderTrainingData: Boolean,
                                     OldTrainingDataEpoch: Int,
                                     AttributionClickLookbackWindowInSeconds: Int,
                                     AttributionImpressionLookbackWindowInSeconds: Int
                                    )

case class AdGroupPolicyDataset(experimentName: String = "") extends KongMingDataset[AdGroupPolicyRecord](
  s3DatasetPath = "dailyadgrouppolicy/v=2",
  experimentName = config.getString("ttd.AdGroupPolicyDataset.experimentName", experimentName)
)