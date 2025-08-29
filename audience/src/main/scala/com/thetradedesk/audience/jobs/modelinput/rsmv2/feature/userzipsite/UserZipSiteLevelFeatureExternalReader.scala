package com.thetradedesk.audience.jobs.modelinput.rsmv2.feature.userzipsite
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.getDateStr
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, OptInSeedRecord, UserSiteZipLevelRecord}
import com.thetradedesk.audience.ttdEnv
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset

object UserZipSiteLevelFeatureExternalReader extends UserZipSiteLevelFeatureGetter {
  override def getFeature(rawBidReq: Dataset[BidSideDataRecord],
                          optInSeed: Dataset[OptInSeedRecord],
                          conf: RelevanceModelInputGeneratorJobConfig): Dataset[UserSiteZipLevelRecord] = {
    // todo: create a dataset and use dataset to read
    val dateStr = getDateStr()
    val env = config.getString("FeatureStoreReadEnv", ttdEnv)
    val densityFeatureReadPathWithoutSlash = config.getString("densityFeatureReadPathWithoutSlash", "profiles/source=bidsimpression/index=TDID/job=DailyTDIDDensityScoreSplitJob/v=1")

    spark.read.parquet(s"s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/${env}/${densityFeatureReadPathWithoutSlash}/date=${dateStr}/split=0")
      .select("TDID", "SyntheticId_Level1", "SyntheticId_Level2")
      .as[UserSiteZipLevelRecord]
  }
}
