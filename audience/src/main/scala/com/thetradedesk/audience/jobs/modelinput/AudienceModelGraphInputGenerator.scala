package com.thetradedesk.audience.jobs.modelinput

import com.thetradedesk.audience.configs.AudienceModelInputGeneratorConfig
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.dateTime
import com.thetradedesk.audience.transform.ExtendArrayTransforms.{seedIdToSyntheticIdMapping, seedIdToSyntheticIdWithSamplingMapping}
import com.thetradedesk.audience.transform.WeightPair
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

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
      .filter(col("IsOriginal").isNotNull)
      .filter(samplingFunction('TDID))
      .repartition(AudienceModelInputGeneratorConfig.bidImpressionRepartitionNumAfterFilter, 'TDID)

    val seedIdToSyntheticId = policyTable
      .filter(e => e.CrossDeviceVendorId == crossDeviceVendor.id)
      .map(e => (e.SourceId, e.SyntheticId))
      .toMap

    val mapping = seedIdToSyntheticIdMapping(seedIdToSyntheticId)

    // extend seeds with Graph
    if (AudienceModelInputGeneratorConfig.enableGraphInRSMETL && crossDeviceVendor == CrossDeviceVendor.None && DataSource.Seed.id == policyTable(0).Source) {
      val seedIdsInBatch = policyTable.map(_.SourceId).toSet
      val graphExtendPolicyTable: Array[GraphChoicePolicy] = graphChoicesInPolicy(date).filter(e => seedIdsInBatch.contains(e.sourceId))
      if (graphExtendPolicyTable.length > 0) {
        val personGraphMapping = seedIdToSyntheticIdWithSamplingMapping(graphExtendPolicyTable.filter(e => e.crossDeviceVendorId == CrossDeviceVendor.IAV2Person.id).map(e => (e.sourceId, WeightPair(e.syntheticId, e.weight))).toMap)
        val houseHoldGraphMapping = seedIdToSyntheticIdWithSamplingMapping(graphExtendPolicyTable.filter(e => e.crossDeviceVendorId == CrossDeviceVendor.IAV2Household.id).map(e => (e.sourceId, WeightPair(e.syntheticId, e.weight))).toMap)
        return seedData
          .select('TDID, 'idType, groupColumn.alias("GroupId"), array_union(mapping(seedIdsColumn), array_union(personGraphMapping('PersonGraphSeedIds), houseHoldGraphMapping('HouseholdGraphSeedIds))).alias("PositiveSyntheticIds"))
          .where(size('PositiveSyntheticIds) > 0)
          .cache()
      }
    }
    seedData
      .select('TDID, 'idType, groupColumn.alias("GroupId"), mapping(seedIdsColumn).alias("PositiveSyntheticIds"))
      .where(size('PositiveSyntheticIds) > 0)
      .cache()
  }

  def graphChoicesInPolicy(date: LocalDate): Array[GraphChoicePolicy] = {
    val campaignSeedDF = CampaignSeedDataset().readLatestPartitionUpTo(date, isInclusive = true)
    val Campaign = CampaignDataSet().readLatestPartitionUpTo(date, isInclusive = true)
    val CCRC = CampaignConversionReportingColumnDataset().readLatestPartitionUpTo(date, isInclusive = true)
    val fullPolicyTable = AudienceModelPolicyReadableDataset(AudienceModelInputGeneratorConfig.model)
      .readSinglePartition(dateTime)(spark).select('SourceId, 'SyntheticId, 'CrossDeviceVendorId, 'ActiveSize)

    val smallSeedId = fullPolicyTable
      .where('CrossDeviceVendorId === lit(CrossDeviceVendor.None.id) && 'ActiveSize <= lit(AudienceModelInputGeneratorConfig.enforceGraphExtensionThreshold))
      .select('SourceId)

    campaignSeedDF
      .select("SeedId", "CampaignId")
      .distinct()
      .join(CCRC, Seq("CampaignId"), "left")
      .withColumn("useHHGraph", when('CrossDeviceAttributionModelId === "IdentityAllianceWithHousehold", 1).otherwise(0))
      .withColumn("usePersonGraph", when('CrossDeviceAttributionModelId === "IdentityAlliance", 1).otherwise(0))
      .withColumn("noGraph", when('CrossDeviceAttributionModelId.isNull, 1).otherwise(0))
      .groupBy("SeedId", "CampaignId")
      .agg(sum("useHHGraph").alias("useHHGraphCount")
        , sum("usePersonGraph").alias("usePersonGraphCount")
        , sum("noGraph").alias("noGraphCount")
      )
      .withColumn("dominateUse", greatest("useHHGraphCount", "usePersonGraphCount", "noGraphCount"))
      .withColumn("graphOfChoiceId",
        //when there's ties choose IAV2 person first then HH then no graph
        when('usePersonGraphCount === 'dominateUse, CrossDeviceVendor.IAV2Person.id)
          .when('useHHGraphCount === 'dominateUse, CrossDeviceVendor.IAV2Household.id)
          .otherwise(CrossDeviceVendor.None.id)
      )
      .join(Campaign.select("CampaignId", "PrimaryChannelId"), Seq("CampaignId"), "left")
      .withColumn("graphOfChoiceId",
        //"PrimaryChannelId"=4 is TV as a channel
        when(('graphOfChoiceId === lit(CrossDeviceVendor.None.id)) && ('PrimaryChannelId === lit(4)), CrossDeviceVendor.IAV2Household.id)
          .otherwise('graphOfChoiceId)
      )
      // select graph for seed
      .groupBy('SeedId)
      .agg(sum(when('graphOfChoiceId === lit(CrossDeviceVendor.IAV2Household.id), 1).otherwise(0)).alias("useHHGraphCount")
        , sum(when('graphOfChoiceId === lit(CrossDeviceVendor.IAV2Person.id), 1).otherwise(0)).alias("usePersonGraphCount")
        , sum(when('graphOfChoiceId === lit(CrossDeviceVendor.None.id), 1).otherwise(0)).alias("noGraphCount")
      )
      .withColumn("dominateUse", greatest("useHHGraphCount", "usePersonGraphCount", "noGraphCount"))
      .withColumn("graphOfChoiceId",
        // when there's ties choose IAV2 person first then HH then no graph
        when('usePersonGraphCount === 'dominateUse, CrossDeviceVendor.IAV2Person.id)
          .when('useHHGraphCount === 'dominateUse, CrossDeviceVendor.IAV2Household.id)
          .otherwise(CrossDeviceVendor.None.id)
      )
      .where('graphOfChoiceId =!= lit(CrossDeviceVendor.None.id))
      .select('SeedId.alias("SourceId"), 'graphOfChoiceId.alias("CrossDeviceVendorId"))
      // apply person graph to small seeds
      .join(smallSeedId, Seq("SourceId"), "full")
      .select('SourceId, coalesce('CrossDeviceVendorId, lit(CrossDeviceVendor.IAV2Person.id)).alias("CrossDeviceVendorId"))
      // join policy table to get active size for sampling
      .join(fullPolicyTable.select('SourceId, 'CrossDeviceVendorId, 'ActiveSize.alias("ExtendedActiveSize")), Seq("SourceId", "CrossDeviceVendorId"), "inner")
      .join(fullPolicyTable.where('CrossDeviceVendorId === lit(CrossDeviceVendor.None.id)).drop("CrossDeviceVendorId"),
        Seq("SourceId"))
      .withColumn("Weight", when('ActiveSize <= lit(AudienceModelInputGeneratorConfig.enforceGraphExtensionThreshold), 1).otherwise('ActiveSize * lit(AudienceModelInputGeneratorConfig.graphExtensionDataRatio) / ('ExtendedActiveSize - 'ActiveSize)).cast(FloatType))
      .select('SourceId, 'SyntheticId, 'CrossDeviceVendorId,'Weight)
      .as[GraphChoicePolicy]
      .collect()
  }
}

case class GraphChoicePolicy(sourceId: String, syntheticId: Int, crossDeviceVendorId: Int, weight: Float)