package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.transform.ExtendArrayTransforms.seedIdToSyntheticIdMapping
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import java.time.LocalDate

abstract class AudienceModelGraphInputGenerator(name: String, crossDeviceVendor: CrossDeviceVendor, override val sampleRate: Double) extends AudienceModelInputGenerator(name, sampleRate){
  lazy val groupColumn: Column = crossDeviceVendor match {
    case CrossDeviceVendor.None => col("TDID")
    case CrossDeviceVendor.IAV2Person => col("personId")
    case CrossDeviceVendor.IAV2Household => col("householdId")
    case _ => throw new RuntimeException(s"cross device vendor ${crossDeviceVendor} is not supportted!")
  }

  lazy val seedIdsColumn: Column = crossDeviceVendor match {
    case CrossDeviceVendor.None => col("SeedIds")
    case CrossDeviceVendor.IAV2Person => array_union(col("SeedIds"), col("PersonGraphSeedIds"))
    case CrossDeviceVendor.IAV2Household => array_union(col("SeedIds"), col("HouseholdGraphSeedIds"))
    case _ => throw new RuntimeException(s"cross device vendor ${crossDeviceVendor} is not supportted!")
  }

  def getAggregatedSeedReadableDataset() : LightReadableDataset[AggregatedSeedRecord]

  override def generateLabels(date: LocalDate, policyTable: Array[AudienceModelPolicyRecord]):
  DataFrame = {
    val seedData = getAggregatedSeedReadableDataset()
      .readPartition(date)(spark)
      .filter(samplingFunction('TDID))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)

    val seedIdToSyntheticId = policyTable
      .filter(e => e.CrossDeviceVendorId == crossDeviceVendor.id)
      .map(e => (e.SourceId, e.SyntheticId))
      .toMap

    val mapping = seedIdToSyntheticIdMapping(seedIdToSyntheticId)

    seedData
      .select('TDID, groupColumn.alias("GroupId"), mapping(seedIdsColumn).alias("PositiveSyntheticIds"))
      .where(size('PositiveSyntheticIds) > 0)
      .cache()
  }

}
