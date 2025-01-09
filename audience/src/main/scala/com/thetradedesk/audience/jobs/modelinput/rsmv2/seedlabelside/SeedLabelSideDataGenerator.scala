package com.thetradedesk.audience.jobs.modelinput.rsmv2.seedlabelside

import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, OptInSeedRecord, SeedLabelSideDataRecord, UserSiteZipLevelRecord}
import org.apache.spark.sql.Dataset

trait SeedLabelSideDataGenerator {

  def prepareSeedSideFeatureAndLabel(optInSeed: Dataset[OptInSeedRecord],
                                     bidSideData: Dataset[BidSideDataRecord], userFs: Dataset[UserSiteZipLevelRecord]): Dataset[SeedLabelSideDataRecord]

}
