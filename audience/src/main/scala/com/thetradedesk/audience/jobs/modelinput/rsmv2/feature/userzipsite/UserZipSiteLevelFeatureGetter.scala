package com.thetradedesk.audience.jobs.modelinput.rsmv2.feature.userzipsite

import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, OptInSeedRecord, UserSiteZipLevelRecord}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig
import org.apache.spark.sql.Dataset

trait UserZipSiteLevelFeatureGetter {
  def getFeature(rawBidReq: Dataset[BidSideDataRecord],
                 optInSeed: Dataset[OptInSeedRecord],
                 conf: RelevanceModelInputGeneratorJobConfig): Dataset[UserSiteZipLevelRecord]
}
