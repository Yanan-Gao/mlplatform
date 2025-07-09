package com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed
import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, CampaignDataSet, CampaignSeedDataset, CrossDeviceVendor, Model}
import com.thetradedesk.audience.{date, dateTime}
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.OptInSeedRecord
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorJobConfig

import java.time.format.DateTimeFormatter

object ActiveSeedGenerator extends OptInSeedGenerator {
  override def generate(conf: RelevanceModelInputGeneratorJobConfig): Dataset[OptInSeedRecord] = {
    val seedId2SyntheticId = AudienceModelPolicyReadableDataset(Model.RSM).readSinglePartition(dateTime)
      .filter('CrossDeviceVendorId === CrossDeviceVendor.None.id && 'IsActive)
      .filter('ActiveSize * conf.RSMV2UserSampleRatio >= conf.lowerLimitPosCntPerSeed * 10)
      .withColumnRenamed("SourceId", "SeedId")
      .select("SeedId", "SyntheticId")
    val startDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(2))
    val endDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)

    val campaignSeed = CampaignSeedDataset().readLatestPartitionUpTo(date)

    val campaign = CampaignDataSet().readLatestPartitionUpTo(date)
      .filter('StartDate.leq(startDateTimeStr)
        && ('EndDate.isNull || 'EndDate.gt(endDateTimeStr)))
      .select('CampaignId)

    val whitelistSeedIds = conf.activeSeedIdWhiteList.split(",").toSeq.toDF("SeedId")

    val optInSeed = campaignSeed
      .join(campaign, Seq("CampaignId"), "inner")
      .select('SeedId)
      .union(whitelistSeedIds.select('SeedId))
      .distinct()

    seedId2SyntheticId.join(optInSeed, "SeedId")
      .as[OptInSeedRecord]
  }
}
