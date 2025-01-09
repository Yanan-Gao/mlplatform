package com.thetradedesk.audience.jobs.modelinput.rsmv2.feature.userzipsite
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.getDateStr
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, OptInSeedRecord, UserSiteZipLevelRecord}
import com.thetradedesk.audience.ttdEnv
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset

object UserZipSiteLevelFeatureExternalReader extends UserZipSiteLevelFeatureGetter {
  override def getFeature(rawBidReq: Dataset[BidSideDataRecord], optInSeed: Dataset[OptInSeedRecord]): Dataset[UserSiteZipLevelRecord] = {
    // todo: create a dataset and use dataset to read
    val dateStr = getDateStr()
    val env = config.getString(s"FeatureStoreReadEnv", ttdEnv)
    val path = if (env == "prodTest") "feature/intermediate" else "profiles"
    spark.read.parquet(s"s3a://thetradedesk-mlplatform-us-east-1/features/feature_store/${env}/${path}/source=bidsimpression/index=TDID/config=TDIDDensityScoreSplit/v=1/date=${dateStr}/split=0")
      .select("TDID", "SyntheticId_Level1", "SyntheticId_Level2")
      .as[UserSiteZipLevelRecord]
  }
}
