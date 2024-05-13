package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.constants.FeatureConstants
import com.thetradedesk.featurestore.ttdEnv

case class UserImpressionFeature(TDID: String,
                                 seenCount1D: Int,
                                 avgCost1D: Float,
                                 totalCost1D: Float,
                                 maxFloorPrice: Float
                                )

case class UserImpressionFeatureDataset() extends LightWritableDataset[UserImpressionFeature] {
  override val defaultNumPartitions: Int = 1024
  override val dataSetPath: String = s"features/feature_store/${ttdEnv}/user_bid_impression_feature/v=1"
  override val rootPath: String = FeatureConstants.ML_PLATFORM_S3_PATH
}
