package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGeneratorConfig
import com.thetradedesk.audience.utils.{MapDensity, S3Utils, SeedPolicyUtils}
import com.thetradedesk.spark.datasets.sources.ThirdPartyDataDataSet
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.AnnotatedSchemaBuilder
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{transform => sql_transform, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RSMGraphPolicyTableGenerator(
    prometheus: PrometheusClient,
    config: AudiencePolicyTableGeneratorConfig)
    extends AudienceGraphPolicyTableGenerator(GoalType.Relevance, Model.RSM, prometheus, config) {

  val rsmSeedProcessCount = prometheus.createCounter("rsm_policy_table_job_seed_process_count", "RSM policy table job seed process record", "seedId", "success")
  private val seedDataSchema = new StructType()
    .add("TDID", StringType)
    .add("SeedIds", ArrayType(StringType))

  private def retrieveSeedMeta(date: LocalDate): DataFrame = {
    val seedDataFullPath = SeedPolicyUtils.getRecentVersion(
      Config.seedMetadataS3Bucket,
      Config.seedMetadataS3Path,
      Config.seedMetaDataRecentVersion
    )

    val country = CountryDataset().readPartition(date).select('ShortName, 'LongName).distinct()
    val countryMap: Map[String, String] = country.collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toMap

    val campaignFlights = CampaignFlightDataSet()
      .readPartition(date)

    val campaigns = CampaignDataSet()
      .readPartition(date)
      .select('CampaignId, 'AdvertiserId)

    val seedMeta =
      if (!Config.allRSMSeed) {
        val startDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(Config.campaignFlightStartingBufferInDays))
        val endDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)

        val activeCampaigns = campaignFlights
          .where('IsDeleted === false
            && 'StartDateInclusiveUTC.leq(startDateTimeStr)
            && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(endDateTimeStr)))
          .select('CampaignId)
          .distinct()

        val campaignSeed = CampaignSeedDataset()
          .readPartition(date)

        val optInSeed = campaignSeed
          .join(activeCampaigns, Seq("CampaignId"), "inner")
          .select('SeedId)
          .distinct()

        spark.read.parquet(seedDataFullPath)
          .join(optInSeed, Seq("SeedId"), "inner")
      } else {
        val activeAdvertiserStartingDateStr = DateTimeFormatter
          .ofPattern("yyyy-MM-dd 00:00:00")
          .format(date.minusDays(Config.activeAdvertiserLookBackDays))

        val activeAdvertisers = campaignFlights
          .filter('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(activeAdvertiserStartingDateStr))
          .select('CampaignId)
          .distinct()
          .join(campaigns, Seq("CampaignId"))
          .select('AdvertiserId)
          .distinct()

        val newSeedTimestamp = Timestamp.valueOf(date.atStartOfDay().minusDays(Config.newSeedLookBackDays))

        spark.read.parquet(seedDataFullPath)
          .join(activeAdvertisers.as("a"), Seq("AdvertiserId"), "left")
          .filter($"a.AdvertiserId".isNotNull || to_timestamp('CreatedAt, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").gt(lit(newSeedTimestamp)))
      }

    seedMeta
      .withColumn("topCountryByDensity", coalesce(MapDensity.getTopDensityFeatures(Config.countryDensityThreshold)('CountByCountry), array()))
      .withColumn("topCountryByDensity", sql_transform(
        col("topCountryByDensity"),
        code => coalesce(
          element_at(typedLit(countryMap), code),
          code // Keep the original code if no mapping is found
        )
      )
      )
      .select('SeedId, 'TargetingDataId, coalesce('Count, lit(-1)).alias("Count"), 'Path, 'topCountryByDensity
        , when(size('FirstPartyInclusionTargetingDataIds) =!= lit(0) || size('RetailInclusionTargetingDataIds) =!= lit(0),
          PermissionTag.Private.id).otherwise(PermissionTag.Shared.id).alias("PermissionTag"))
      .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
      .cache()

  }

  override def retrieveSourceMetaData(date: LocalDate): Dataset[SourceMetaRecord] = {
    val ttdRawDataMeta = retrieveTTDRawDataMeta(date)
    val seedDataMeta = retrieveSeedDataMeta(date).where('Count >= lit(Config.seedProcessLowerThreshold) && 'Count <= lit(Config.seedProcessUpperThreshold))
    ttdRawDataMeta.union(seedDataMeta)
  }

  private def retrieveSeedDataMeta(date: LocalDate): Dataset[SourceMetaRecord] = {
    val seedMeta = retrieveSeedMeta(date)
    seedMeta
      .withColumnRenamed("SeedId", "SourceId").select('SourceId, 'Count, 'TargetingDataId, 'topCountryByDensity, lit(DataSource.Seed.id).alias("Source"), 'PermissionTag).as[SourceMetaRecord]
  }

  private def retrieveTTDRawDataMeta(date: LocalDate): Dataset[SourceMetaRecord] = {
    val segmentsSummary = spark.read.format("com.thetradedesk.segment.client.spark_3.receivedSegmentSummary.ReceivedSegmentSummaryTableProvider").load()

    val metaTable = ThirdPartyDataDataSet()
      .readPartition(date)
      .where('ThirdPartyDataHierarchyString.startsWith("/2537086/242456787/")) // means TTD own segments
      .select('TargetingDataId)
      .join(segmentsSummary, Seq("TargetingDataId"), "inner")
      .where('RecordCount >= lit(Config.seedProcessLowerThreshold) &&
        'RecordCount <= lit(Config.ttdOwnDataUpperThreshold))
      .select('TargetingDataId.cast(StringType).alias("SourceId"), 'RecordCount.alias("Count"), 'TargetingDataId, array(lit("")).alias("topCountryByDensity"))
    metaTable
      .withColumn("Source", lit(DataSource.TTDOwnData.id))
      .withColumn("PermissionTag", lit(PermissionTag.None.id)).as[SourceMetaRecord]
  }

  override def retrieveSourceDataWithDifferentGraphType(date: LocalDate, personGraph: DataFrame, householdGraph: DataFrame): SourceDataWithDifferentGraphType = {
    val seedSourceDataWithDifferentGraphType = retrieveSeedDataWithDifferentGraphType(date, personGraph, householdGraph)
    val ttdSourceRawData = retrieveTTDRawData(date)
    SourceDataWithDifferentGraphType(
      seedSourceDataWithDifferentGraphType.NoneGraphData
        .join(ttdSourceRawData._1.withColumnRenamed("SeedIds", "SeedIds1"), Seq("TDID"), "outer")
        .select('TDID, concat(coalesce('SeedIds, typedLit(Array.empty[String])), coalesce('SeedIds1, typedLit(Array.empty[String]))).alias("SeedIds"))
        .as[AggregatedGraphTypeRecord],
      seedSourceDataWithDifferentGraphType.PersonGraphData,
      seedSourceDataWithDifferentGraphType.HouseholdGraphData,
      seedSourceDataWithDifferentGraphType.SourceMeta.union(ttdSourceRawData._2)
    )
  }

  private def retrieveTTDRawData(date: LocalDate): (Dataset[AggregatedGraphTypeRecord], Dataset[SourceMetaRecord]) = {
    val segmentsSummary = spark.read.format("com.thetradedesk.segment.client.spark_3.receivedSegmentSummary.ReceivedSegmentSummaryTableProvider").load()

    val metaTable = ThirdPartyDataDataSet()
      .readPartition(date)
      .where('ThirdPartyDataHierarchyString.startsWith("/2537086/242456787/")) // means TTD own segments
      .withColumn(
        "ttdOwnDataUpperThreshold",
        when(
          not(col("ThirdPartyDataHierarchyString").like("/2537086/242456787/%/%/%")),
          lit(Config.ttdOwnDataUpperThreshold)
        ).otherwise(
          lit(Config.seedProcessUpperThreshold)
        )
      )
      .select('TargetingDataId, 'ttdOwnDataUpperThreshold)
      .join(segmentsSummary, Seq("TargetingDataId"), "inner")
      .where('RecordCount >= lit(Config.seedProcessLowerThreshold) &&
        'RecordCount <= 'ttdOwnDataUpperThreshold)
      .select('TargetingDataId, 'RecordCount.alias("Count"), 'TargetingDataId.alias("SourceId"))

    val tdid2TargetingDataIds = spark.read.format("com.thetradedesk.segment.client.spark_3.receivedSegment.ReceivedSegmentTableProvider")
      .option("targetingDataIds", metaTable.select('TargetingDataId).as[Long].collect().mkString(","))
      .load()
      .groupBy('UserId.alias("TDID"))
      .agg(collect_list('TargetingDataId).alias("TargetingDataIds"))

    (tdid2TargetingDataIds.select('TDID, org.apache.spark.sql.functions.transform('TargetingDataIds, item => item.cast(StringType)).alias("SeedIds")).as[AggregatedGraphTypeRecord],
      metaTable.withColumn("topCountryByDensity", array(lit(""))).withColumn("Source", lit(DataSource.TTDOwnData.id)).withColumn("PermissionTag", lit(PermissionTag.None.id)).as[SourceMetaRecord])
  }

  def retrieveSeedDataWithDifferentGraphType(date: LocalDate, personGraph: DataFrame, householdGraph: DataFrame): SourceDataWithDifferentGraphType = {
    val seedMeta = retrieveSeedMeta(date)

    val policyMetaTable = seedMeta
      .drop("topCountryByDensity", "PermissionTag")
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
      seedMeta.withColumnRenamed("SeedId", "SourceId").select('SourceId, 'Count, 'TargetingDataId, 'topCountryByDensity, lit(DataSource.Seed.id).alias("Source"), 'PermissionTag).as[SourceMetaRecord]
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
