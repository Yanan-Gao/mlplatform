package com.thetradedesk.audience.jobs.modelinput.rsmv2.optinseed
import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, CrossDeviceVendor, Model}
import com.thetradedesk.audience.dateTime
import com.thetradedesk.audience.jobs.modelinput.rsmv2.datainterface.OptInSeedRecord
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.audience.jobs.modelinput.rsmv2.RelevanceModelInputGeneratorConfig.{RSMV2UserSampleRatio, lowerLimitPosCntPerSeed}

class DynamicSeedGenerator(private val filterExpr: String = "true")
  extends OptInSeedGenerator {

  override def generate(): Dataset[OptInSeedRecord] = {
    AudienceModelPolicyReadableDataset(Model.RSM).readSinglePartition(dateTime)
      .filter('CrossDeviceVendorId === CrossDeviceVendor.None.id && 'IsActive)
      .filter('ActiveSize * RSMV2UserSampleRatio >= lowerLimitPosCntPerSeed * 10)
      .filter(filterExpr)
      .withColumnRenamed("SourceId", "SeedId")
      .select("SeedId", "SyntheticId")
      .as[OptInSeedRecord]
  }
}
