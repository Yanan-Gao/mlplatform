package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.configs.UserFeatureMergeDefinition

case class SeedDensityFeature(
                               FeatureKeyValueHashed: String,
                               data: Array[Byte]
                             )

case class SeedDensityFeatureDataset(userFeatureMergeDefinition: UserFeatureMergeDefinition)
  extends CustomBufferDataset[SeedDensityFeature](userFeatureMergeDefinition)
