package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGeneratorJob.prometheus
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.AnnotatedSchemaBuilder
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object RSMGraphPolicyTableGenerator extends AudienceGraphPolicyTableGenerator(GoalType.Relevance, DataSource.Seed, Model.RSM, prometheus: PrometheusClient) {

  val rsmSeedProcessCount = prometheus.createCounter("rsm_policy_table_job_seed_process_count", "RSM policy table job seed process record", "seedId", "success")
  private val seedDataSchema = new StructType()
    .add("TDID", StringType)
    .add("SeedIds", ArrayType(StringType))

  private def retrieveSeedMeta(date: LocalDate): DataFrame = {
    val recentVersion =
      if (Config.seedMetaDataRecentVersion != null) Config.seedMetaDataRecentVersion
      else S3Utils.queryCurrentDataVersion(Config.seedMetadataS3Bucket, Config.seedMetadataS3Path)

    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + "/" + Config.seedMetadataS3Path + "/" + recentVersion

    val seedMeta =
      if (!Config.allRSMSeed) {
        val startDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(Config.campaignFlightStartingBufferInDays))
        val endDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)

        val campaignFlight = CampaignFlightDataSet()
          .readPartition(date)
          .where('IsDeleted === false
            && 'StartDateInclusiveUTC.leq(startDateTimeStr)
            && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(endDateTimeStr)))
          .select('CampaignId)
          .distinct()

        val campaignSeed = CampaignSeedDataset()
          .readPartition(date)

        val optInSeed = campaignSeed
          .join(campaignFlight, Seq("CampaignId"), "inner")
          .select('SeedId)
          .distinct()

        spark.read.parquet(seedDataFullPath)
          .join(optInSeed, Seq("SeedId"), "inner")
          .select('SeedId, 'TargetingDataId, coalesce('Count, lit(-1)).alias("Count"), 'Path)
          .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
          .cache()
      } else {
        spark.read.parquet(seedDataFullPath)
          .select('SeedId, 'TargetingDataId, coalesce('Count, lit(-1)).alias("Count"), 'Path)
          .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
          .cache()
      }

    seedMeta

  }

  override def retrieveSourceDataWithDifferentGraphType(date: LocalDate, personGraph: DataFrame, householdGraph: DataFrame): SourceDataWithDifferentGraphType = {
    val seedMeta = retrieveSeedMeta(date)

    val policyMetaTable = seedMeta
      .as[SeedDetail]
      .collect()

    // TODO support seed data cache

    policyMetaTable
      .filter(e => e.Count < Config.seedProcessLowerThreshold || e.Count > Config.seedProcessUpperThreshold)
      .foreach(e => rsmSeedProcessCount.labels(e.SeedId, "Cutoff").inc())

    val largeSeedData = handleLargeSeedData(policyMetaTable.filter(e => e.Count > Config.seedExtendGraphUpperThreshold && e.Count <= Config.seedProcessUpperThreshold).map(i => i.Path))

    val smallSeedData = handleSmallSeedData(personGraph, householdGraph, policyMetaTable.filter(e => e.Count >= Config.seedProcessLowerThreshold && e.Count <= Config.seedExtendGraphUpperThreshold).map(i => i.Path))

    SourceDataWithDifferentGraphType(
      largeSeedData.union(smallSeedData._1),
      smallSeedData._2,
      smallSeedData._3,
      seedMeta.withColumnRenamed("SeedId","SourceId").select('SourceId,'Count, 'TargetingDataId).as[SourceMetaRecord]
    )

  }

  override def getAggregatedSeedWritableDataset(): LightWritableDataset[AggregatedSeedRecord] = AggregatedSeedWritableDataset()

  override def getAggregatedSeedReadableDataset(): LightReadableDataset[AggregatedSeedRecord] = AggregatedSeedReadableDataset()

  private def retrieveSeedData(paths: Array[String], isSample: Boolean): (DataFrame, DataFrame) = {
    val basePath = "s3://" + Config.seedRawDataS3Bucket + "/" + Config.seedRawDataS3Path
    val seedDataSet =
      if (isSample) {
        spark.read.option("basePath", basePath)
          .schema(AnnotatedSchemaBuilder.schema[SeedRecord])
          .parquet(paths: _*)
          .where(samplingFunction('UserId))
          .select('UserId.alias("TDID"), 'SeedId)
      } else {
        spark.read.option("basePath", basePath)
          .schema(AnnotatedSchemaBuilder.schema[SeedRecord])
          .parquet(paths: _*)
          .select('UserId.alias("TDID"), 'SeedId)
      }
    val TDID2Seeds = seedDataSet
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
      .groupBy('TDID)
      .agg(collect_set('SeedId).alias("SeedIds"))

    (TDID2Seeds, seedDataSet)
  }

  private def handleLargeSeedData(policyMetaTable: Array[String]): Dataset[AggregatedGraphTypeRecord] = {
    if (policyMetaTable.isEmpty) {
      return spark.createDataFrame(spark.sparkContext
        .emptyRDD[Row], seedDataSchema).as[AggregatedGraphTypeRecord]
    }
    val tempPolicyMetaTable = if (dryRun) policyMetaTable.take(10) else policyMetaTable
    retrieveSeedData(tempPolicyMetaTable, isSample = true)._1.as[AggregatedGraphTypeRecord]
  }

  private def handleSmallSeedData(personGraph: DataFrame, householdGraph: DataFrame, policyMetaTable: Array[String]):
  (Dataset[AggregatedGraphTypeRecord], Dataset[AggregatedGraphTypeRecord], Dataset[AggregatedGraphTypeRecord]) = {
    if (policyMetaTable.isEmpty) {
      return (spark.createDataFrame(spark.sparkContext
        .emptyRDD[Row], seedDataSchema).as[AggregatedGraphTypeRecord],
        spark.createDataFrame(spark.sparkContext
          .emptyRDD[Row], seedDataSchema).as[AggregatedGraphTypeRecord],
        spark.createDataFrame(spark.sparkContext
          .emptyRDD[Row], seedDataSchema).as[AggregatedGraphTypeRecord])
    }

    val tempPolicyMetaTable = if (dryRun) policyMetaTable.take(10) else policyMetaTable

    val seedData = retrieveSeedData(tempPolicyMetaTable, isSample = false)

    val personGraphSeedData = readGraphDataAndExtendSeed(seedData._2, personGraph)(spark)
    val householdGraphSeedData = readGraphDataAndExtendSeed(seedData._2, householdGraph)(spark)

    val sampledSeedData = seedData._1
      .where(samplingFunction('TDID))
      .as[AggregatedGraphTypeRecord]

    (sampledSeedData, personGraphSeedData, householdGraphSeedData)
  }

  private def readGraphDataAndExtendSeed(seedData: DataFrame, graph: DataFrame)(implicit spark: SparkSession): Dataset[AggregatedGraphTypeRecord] = {
    val seedWithGraph = graph.join(seedData, Seq("TDID"), "inner")

    val groupIdToSeedIds = seedWithGraph
      .groupBy('groupId)
      .agg(collect_set('SeedId).alias("SeedIds"))

    val graphGroup = graph
      .groupBy('groupId)
      .agg(collect_set('TDID).alias("TDIDs"))
      .select('groupId, arraySamplingFunction('TDIDs).alias("TDIDs"))
      .where(size('TDIDs) > lit(0))

    val TDID2Seeds = groupIdToSeedIds
      .join(graphGroup, Seq("groupId"), "inner")
      .select(explode('TDIDs).alias("TDID"), 'SeedIds)

    TDID2Seeds.as[AggregatedGraphTypeRecord]
  }

}
