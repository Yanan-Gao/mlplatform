package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.configs.UserFeatureMergeDefinition

case class UserFeature(TDID: String,
                       data: Array[Byte]
                      )

case class UserFeatureDataset(userFeatureMergeDefinition: UserFeatureMergeDefinition)
  extends CustomBufferDataset[UserFeature](userFeatureMergeDefinition)