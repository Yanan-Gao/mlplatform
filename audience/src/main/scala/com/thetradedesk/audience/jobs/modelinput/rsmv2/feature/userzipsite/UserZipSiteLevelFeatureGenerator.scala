package com.thetradedesk.audience.jobs.modelinput.rsmv2.feature.userzipsite

import com.thetradedesk.audience.datasets.AggregatedSeedReadableDataset
import com.thetradedesk.audience.date
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RSMV2SharedFunction.writeOrCache
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.{BidSideDataRecord, OptInSeedRecord, SiteZipDensityRecord, UserSiteZipLevelRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.TTDSparkContext.spark

object UserZipSiteLevelFeatureGenerator extends UserZipSiteLevelFeatureGetter {

  private def generateSiteZipScore(featureRelatedBidReq: DataFrame,
                                   optInSeed: Dataset[OptInSeedRecord],
                                   conf: RelevanceModelInputGeneratorJobConfig): Dataset[SiteZipDensityRecord] = {

    val seedIdToSyntheticId = optInSeed.collect()
      .map(e => (e.SeedId, e.SyntheticId))
      .toMap

    val mapping = RSMV2SharedFunction.seedIdToSyntheticIdMapping(seedIdToSyntheticId)

    val aggregatedSeed = AggregatedSeedReadableDataset().readPartition(date)
      .filter(col("IsOriginal").isNotNull)
      .withColumn("SyntheticIds", mapping(col("SeedIds")))
      .select("TDID", "SyntheticIds")

    val seedDensity =
      featureRelatedBidReq.join(aggregatedSeed, "TDID").withColumn("SyntheticId", explode(col("SyntheticIds"))).groupBy("SyntheticId", "Site", "Zip").count()
        .withColumnRenamed("Count", "SeedCount")
        .withColumn("TotalCount", sum("SeedCount").over(Window.partitionBy("SyntheticId")))
        .withColumn("InDensity", col("SeedCount") / col("TotalCount"))

    val generalPopulationFrequencyMap = featureRelatedBidReq.groupBy("Site", "Zip").count()
    val totalCnt = generalPopulationFrequencyMap.agg(sum("count")).first().getLong(0)

    val sitezipScore = generalPopulationFrequencyMap
      .withColumnRenamed("Count", "PopCount")
      .join(seedDensity, Seq("Site", "Zip"))
      .withColumn("OutDensity", ('PopCount - 'SeedCount) / (lit(totalCnt) - 'TotalCount))
      .withColumn("score", 'InDensity / ('InDensity + 'OutDensity))
      .select("Site", "Zip", "SyntheticId", "score")

    val dateStr = RSMV2SharedFunction.getDateStr()
    val writePath = s"s3://thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/allinone/dataset/${dateStr}/features/site_zip_score"
    writeOrCache(Option(writePath),
      conf.overrideMode, sitezipScore, false).as[SiteZipDensityRecord]
  }

  private def generateUserScore(siteZipFeatureStore: Dataset[SiteZipDensityRecord],
                                tdid2feature: DataFrame,
                                conf: RelevanceModelInputGeneratorJobConfig): Dataset[UserSiteZipLevelRecord] = {
    val szFs = siteZipFeatureStore.filter('score >= 0.8)
    val userFs =
      tdid2feature
        .join(szFs, Seq("Site", "Zip"))
        .groupBy("TDID", "SyntheticId")
        .agg(max("score").alias("score"))
        .withColumn("SiteZipLevel", when('score >= 0.99, 2).otherwise(1))
        .groupBy("TDID")
        .agg(
          collect_set(when('SiteZipLevel === 2, 'SyntheticId)).alias("SyntheticId_Level2"),
          collect_set(when('SiteZipLevel === 1, 'SyntheticId)).alias("SyntheticId_Level1")
        )
    val dateStr = RSMV2SharedFunction.getDateStr()
    writeOrCache(Option(s"s3://thetradedesk-mlplatform-us-east-1/users/yixuan.zheng/allinone/dataset/${dateStr}/features/user_sz_score"),
      conf.overrideMode, userFs).as[UserSiteZipLevelRecord]
  }

  override def getFeature(rawBidReq: Dataset[BidSideDataRecord],
                          optInSeed: Dataset[OptInSeedRecord],
                          conf: RelevanceModelInputGeneratorJobConfig): Dataset[UserSiteZipLevelRecord] = {
    val featureRelatedBidReq = rawBidReq.select("TDID", "Site", "Zip")
    val siteZipFeatureStore = generateSiteZipScore(featureRelatedBidReq, optInSeed, conf)

    val tdid2feature = rawBidReq.select("TDID", "Site", "Zip").distinct()

    generateUserScore(siteZipFeatureStore, tdid2feature, conf)
  }
}
