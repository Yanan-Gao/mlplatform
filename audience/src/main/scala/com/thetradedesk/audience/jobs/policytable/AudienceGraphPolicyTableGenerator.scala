package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets.CrossDeviceVendor.{CrossDeviceVendor, IAV2Person}
import com.thetradedesk.audience.datasets.DataSource.DataSource
import com.thetradedesk.audience.datasets.GoalType.GoalType
import com.thetradedesk.audience.datasets.StorageCloud
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets.PermissionTag.PermissionTag
import com.thetradedesk.audience.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.audience.utils.GraphPolicyTableUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate

/*
IMPORTANT:
seed here don't represent kokai seed, it represents any pixel(group of tdid) in TTD (including kokai seed, conversion tracker etc.)
*/
abstract class AudienceGraphPolicyTableGenerator(goalType: GoalType, model: Model) extends AudiencePolicyTableGenerator(model) {
  def retrieveSourceMetaData(date: LocalDate): Dataset[SourceMetaRecord]

  def retrieveSourceDataWithDifferentGraphType(date: LocalDate, personGraph: DataFrame, householdGraph: DataFrame): SourceDataWithDifferentGraphType

  def getAggregatedSeedWritableDataset(): LightWritableDataset[AggregatedSeedRecord]

  def getAggregatedSeedReadableDataset(): LightReadableDataset[AggregatedSeedRecord]


  def generateRawPolicyTable(sourceMeta: Dataset[SourceMetaRecord], date: LocalDate): DataFrame = {
    val finalSeedData = getAggregatedSeedReadableDataset().readPartition(date)(spark).withColumnRenamed("SeedIds", "SourceIds")

    GraphPolicyTableUtils
      .generateRawPolicyTable(
        sourceMeta,
        finalSeedData,
        userDownSampleBasePopulation,
        userDownSampleHitPopulation,
        goalType.id,
        Config.storageCloud
      )
  }

  override def retrieveSourceData(date: LocalDate): DataFrame = {
    val successFile = getAggregatedSeedReadableDataset().DatePartitionedPath(Some(date)) + "/_SUCCESS"
    if (Config.reuseAggregatedSeedIfPossible && FSUtils.fileExists(successFile)(spark)) {
      val sourceMeta = retrieveSourceMetaData(date)
      // step 5. generate policy table
      return generateRawPolicyTable(sourceMeta, date)
    }

    // step 1. fetch unique tdid
    val uniqueTDIDs = getBidImpUniqueTDIDs(date)

    // step 2. fetch graph data
    val personGraph = readGraphData(date, CrossDeviceVendor.IAV2Person)(spark).cache()

    val sampledPersonGraph = personGraph
      .select('TDID, 'groupId)
      .where(samplingFunction('TDID))
      .withColumnRenamed("groupId", "personId")
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)

    val householdGraph = readGraphData(date, CrossDeviceVendor.IAV2Household)(spark).cache()

    val sampledHouseholdGraph = householdGraph
      .select('TDID, 'groupId)
      .where(samplingFunction('TDID))
      .withColumnRenamed("groupId", "householdId")
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)

    val sampledGraph = sampledPersonGraph
      .join(sampledHouseholdGraph, Seq("TDID"), "outer")
      .cache()

    // step 3 generate source data with graph

    val sourceData = retrieveSourceDataWithDifferentGraphType(date, personGraph, householdGraph)

    val allSeedData = sourceData.NoneGraphData
      .groupBy('TDID)
      .agg(collect_list('SeedIds).alias("SeedIds"))
      .select('TDID, flatten('SeedIds).alias("SeedIds"))
      .join(sourceData.PersonGraphData.select('TDID, 'SeedIds.alias("PersonGraphSeedIds")), Seq("TDID"), "outer")
      .join(sourceData.HouseholdGraphData.select('TDID, 'SeedIds.alias("HouseholdGraphSeedIds")), Seq("TDID"), "outer")

    val allFinalSeedData =
      allSeedData.join(uniqueTDIDs, Seq("TDID"), "inner")
        .join(sampledGraph, Seq("TDID"), "left")
        .select('TDID, 'idType, 'IsOriginal,
          coalesce('SeedIds, typedLit(Array.empty[String])).alias("SeedIds"),
          coalesce('PersonGraphSeedIds, typedLit(Array.empty[String])).alias("PersonGraphSeedIds"),
          coalesce('HouseholdGraphSeedIds, typedLit(Array.empty[String])).alias("HouseholdGraphSeedIds"),
          coalesce('personId, 'TDID).alias("personId"),
          coalesce('householdId, 'TDID).alias("householdId"))
        .select('TDID, 'idType, 'IsOriginal, 'SeedIds,
          array_except('PersonGraphSeedIds, 'SeedIds).alias("PersonGraphSeedIds"),
          array_except('HouseholdGraphSeedIds, 'SeedIds).alias("HouseholdGraphSeedIds"),
          'personId,
          'householdId
        )
        .as[AggregatedSeedRecord]

    // step 4. write aggregated seed data
    getAggregatedSeedWritableDataset()
      .writePartition(allFinalSeedData,
        date,
        saveMode = SaveMode.Overwrite)

    // make sure dataset is written successfully to s3
    var checkCount = 0
    while (checkCount < 10 && !FSUtils.fileExists(successFile)(spark)) {
      checkCount += 1
      Thread.sleep(1000 * checkCount)
    }
    if (checkCount == 10) {
      throw new Exception(s"final seed data failed to sync, success file ${successFile}")
    }

    // step 5. generate policy table
    generateRawPolicyTable(sourceData.SourceMeta, date)
  }
}


final case class AggregatedGraphTypeRecord(TDID: String,
                                      SeedIds: Seq[String])

final case class SourceMetaRecord(SourceId: String,
                                  Count: BigInt,
                                  TargetingDataId: BigInt,
                                  topCountryByDensity: Seq[String],
                                  Source: Int,
                                  PermissionTag: PermissionTag
                                  )


case class SourceDataWithDifferentGraphType(
  NoneGraphData: Dataset[AggregatedGraphTypeRecord],
  PersonGraphData: Dataset[AggregatedGraphTypeRecord],
  HouseholdGraphData: Dataset[AggregatedGraphTypeRecord],
  SourceMeta: Dataset[SourceMetaRecord]
)
