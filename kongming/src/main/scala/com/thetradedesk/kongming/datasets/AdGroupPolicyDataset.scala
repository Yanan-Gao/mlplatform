package com.thetradedesk.kongming.datasets

final case class AdGroupPolicyRecord(ConfigKey: String,
                                     ConfigValue: String,
                                     DataAggKey: String,
                                     DataAggValue: String,
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

case class AdGroupPolicyDataset(experimentOverride: Option[String] = None) extends KongMingDataset[AdGroupPolicyRecord](
  s3DatasetPath = "dailyadgrouppolicy/v=2",
  experimentOverride = experimentOverride
)