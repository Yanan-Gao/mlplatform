package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience.{date, _}
import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.transform.IDTransform.{allIdWithType, filterOnIdTypes}
import com.thetradedesk.audience.utils.{S3Utils, SeedPolicyUtils}
import com.thetradedesk.confetti.utils.{CloudWatchLoggerFactory, Logger}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

abstract class AudiencePolicyTableGenerator(
    model: Model,
    prometheus: PrometheusClient,
    confettiEnv: String,
    experimentName: Option[String],
    val conf: AudiencePolicyTableGeneratorConfig) {

  val logger: Logger = CloudWatchLoggerFactory.getLogger(
    s"Confetti-$confettiEnv",
    s"${experimentName.filter(_.nonEmpty).map(n => s"$n-").getOrElse("")}AudiencePolicyTableGenerator"
  )

  val samplingFunction = shouldConsiderTDID3(conf.userDownSampleHitPopulation, conf.saltToSampleUser)(_)
  val arraySamplingFunction = shouldConsiderTDIDInArray3(conf.userDownSampleHitPopulation, conf.saltToSampleUser)
  val jobRunningTime = prometheus.createGauge(s"audience_policy_table_job_running_time", "AudiencePolicyTableGenerator running time", "model", "date")
  val policyTableSize = prometheus.createGauge(s"audience_policy_table_size", "AudiencePolicyTableGenerator running time", "model", "date")

  private val policyTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)

  private val availablePolicyTableVersions = S3Utils
    .queryCurrentDataVersions(conf.policyS3Bucket, conf.policyS3Path)
    .map(LocalDateTime.parse(_, policyTableDateFormatter))
    .toSeq
    .sortWith(_.isAfter(_))

  logger.info("availablePolicyTableVersions: " + availablePolicyTableVersions.mkString(", "))

  def generatePolicyTable(): Unit = {

    validateConfig()

    val date = conf.runDate
    val dateTime = conf.runDate.atStartOfDay()

    val start = System.currentTimeMillis()

    // read the seeddetail data again to join the advertiserid back to here
    val seedDataFullPath = SeedPolicyUtils.getRecentVersion(
      conf.seedMetadataS3Bucket,
      conf.seedMetadataS3Path,
      conf.seedMetaDataRecentVersion // strings like: "DateTime=20250820T015956.000/"
    )
    logger.info("seedDataFullPath: " + seedDataFullPath)

    val seedAdvertiserMetaDataset = spark.read.parquet(seedDataFullPath)
        .select('SeedId.alias("SourceId"), 'AdvertiserId)
        .cache()

    val advertiser = AdvertiserDataSet().readPartition(date)(spark)
    val restrictedAdvertiser = RestrictedAdvertiserDataSet().readPartition(date)(spark)

    // generate needGraphExtension
    val campaign = CampaignDataSet().readPartition(date).select("CampaignId","PrimaryChannelId","BudgetInAdvertiserCurrency","AdvertiserId")
      .join(CampaignFlightDataSet.activeCampaigns(date, startShift = 2, endShift = -1), "CampaignId")
    val campaignSeed = CampaignSeedDataset().readPartition(date)
    val dailyExchangeRateDataset = DailyExchangeRateDataset().readPartition(date)

    val channel_budget = campaign.join(campaignSeed, Seq("CampaignId"))
      .join(advertiser.select("AdvertiserId", "CurrencyCodeId"), Seq("AdvertiserId"), "left")
      .join(dailyExchangeRateDataset, Seq("CurrencyCodeId"), "left")
      .withColumn("BudgetInAdvertiserCurrencyInUSD", col("BudgetInAdvertiserCurrency") / col("FromUSD"))
      .groupBy("SeedId", "PrimaryChannelId")
      .agg(
        sum(col("BudgetInAdvertiserCurrencyInUSD")).alias("channel_budget"),
        count("*").alias("ChannelCampaignCount")
      )

    val seed_total_composite_weight = channel_budget
      .groupBy("SeedId")
      .agg(
        sum(col("channel_budget") * col("ChannelCampaignCount")).alias("TotalCompositeWeight")
      )

    val seedCampaignNeedGE = seed_total_composite_weight
      .join(channel_budget, Seq("SeedId"), "inner")
      .withColumnRenamed("SeedId", "SourceId")
      .withColumn("channel_composite_weight", col("channel_budget") * col("ChannelCampaignCount"))
      .withColumn(
        "NeedGraphExtension",
        when(
          (col("PrimaryChannelId") === 4) &&
            (col("channel_composite_weight") / col("TotalCompositeWeight") > conf.ctvGraphExtensionRatio),
          true
        ).when(
          (col("PrimaryChannelId") === 3) &&
            (col("channel_composite_weight") / col("TotalCompositeWeight") > conf.audioGraphExtensionRatio),
          true
        ).otherwise(false)
      )
      .groupBy("SourceId")
      .agg(max("NeedGraphExtension").alias("NeedGraphExtension"))

    val seedAdvertiserDataset = seedAdvertiserMetaDataset
        .join(advertiser, Seq("AdvertiserId"), "left")
        .join(restrictedAdvertiser, Seq("AdvertiserId"), "left")
        .withColumn(
          "IsSensitive",
          when(col("IsRestricted")===1, lit(true)).otherwise(false)
        ).drop("IsRestricted")
    val recentVersionOption = if (conf.seedMetaDataRecentVersion.isDefined) Some(LocalDateTime.parse(conf.seedMetaDataRecentVersion.get.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
    else availablePolicyTableVersions.find(_.isBefore(dateTime))
    val policyTable = retrieveSourceData(dateTime.toLocalDate)
    val policyTablePrev =AudienceModelPolicyReadableDataset(Model.RSM)
      .readSinglePartition(recentVersionOption.get.asInstanceOf[java.time.LocalDateTime])(spark).select("NeedGraphExtension","SourceId","Source","CrossDeviceVendorId").withColumnRenamed("NeedGraphExtension","PrevNeedGraphExtension")
    val policyTableResult = allocateSyntheticId(dateTime, policyTable)
                            .join(seedAdvertiserDataset, Seq("SourceId"), "left")
                            .withColumn("IsSensitive", coalesce(col("IsSensitive"), lit(false))) // in case ttd segment data does not include in seeddetail
                            .join(seedCampaignNeedGE, Seq("SourceId"), "left")
                            .join(policyTablePrev.as("yesterday"), Seq("SourceId","Source","CrossDeviceVendorId"), "left")
                            .withColumn(
                              "NeedGraphExtension",
                              (col("yesterday.PrevNeedGraphExtension") === true && col("IsActive") === true) ||
                                col("NeedGraphExtension") === true
                            ).drop("channel_budget","weighted_count","PrevNeedGraphExtension","ChannelCampaignCount")
    AudienceModelPolicyWritableDatasetWithExperiment(model, confettiEnv, experimentName)
      .writePartition(
        policyTableResult.as[AudienceModelPolicyRecord],
        dateTime,
        saveMode = SaveMode.Overwrite
      )

    policyTableSize.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(policyTableResult.count())
    jobRunningTime.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }

  private def validateConfig(): Unit = {
    if (ttdEnv == "prod" && conf.policyTableResetSyntheticId == true) {
      throw new IllegalArgumentException("Cannot reset synthetic id for prod env")
    }
  }

  def getBidImpUniqueTDIDs(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val baseDF = loadParquetData[BidsImpressionsSchema](
      bidImpressionsS3Path,
      date,
      lookBack = Some(conf.bidImpressionLookBack),
      source = Some(GERONIMO_DATA_SOURCE)
    ).select('UIID, 'DeviceAdvertisingId, 'CookieTDID, 'IdentityLinkId, 'DATId, 'UnifiedId2, 'EUID, 'IdType)
      .cache()

    val uniqueTDIDsIdExtend = baseDF
      .filter(filterOnIdTypes(samplingFunction))
      .select(allIdWithType.alias("x"))
      .select(col("x._1").as("TDID"), col("x._2").as("idType"))
      .repartition(conf.bidImpressionRepartitionNum, 'TDID)
      .withColumn("rn", row_number().over(Window.partitionBy("TDID").orderBy("idType")))
      .filter(col("rn") === 1) // Keep one idType per TDID following enum ordinal
      .drop("rn")
      .distinct()

    val uniqueTDIDsOriginal = baseDF
      .withColumn("TDID", getUiid('UIID, 'UnifiedId2, 'EUID, 'IdentityLinkId, 'IdType))
      .filter(samplingFunction(col("TDID")))
      .select("TDID")
      .repartition(conf.bidImpressionRepartitionNum, 'TDID)
      .distinct()
      .withColumn("IsOriginal", lit(1))


    uniqueTDIDsIdExtend.join(uniqueTDIDsOriginal, Seq("TDID"), "left")
      .withColumn("IsOriginal", coalesce(col("IsOriginal"), lit(0)))
      .cache()
  }

  def readGraphData(date: LocalDate, crossDeviceVendor: CrossDeviceVendor)(implicit spark: SparkSession): DataFrame = {
    CrossDeviceGraphUtil.readGraphData(date, crossDeviceVendor, conf.graphScoreThreshold, Some(samplingFunction))
  }

  def generateGraphMapping(sourceGraph: DataFrame): DataFrame = {
    val graph = sourceGraph
      .where(shouldTrackTDID('uiid) && 'score > lit(conf.graphScoreThreshold))
      .groupBy('groupId)
      .agg(collect_set('uiid).alias("TDID"))
      .where(size('TDID) > lit(1) && size('TDID) <= lit(conf.graphUniqueCountKeepThreshold)) // remove persons with too many individuals or only one TDID
      .cache()

    val sampledGraph = graph
      .select('groupId, arraySamplingFunction('TDID).alias("co_TDIDs"))
      .where(size('co_TDIDs) > lit(0))

    val mapping = graph
      .join(
        sampledGraph,
        Seq("groupId"),
        "inner")
      .select(explode('TDID).alias("TDID"), 'co_TDIDs)
      .select('TDID, array_remove('co_TDIDs, 'TDID).alias("co_TDIDs"))
      .where(size('co_TDIDs) > lit(0))

    mapping
  }

  // todo assign synthetic id, weight, tag, and isActive

  private def updateSyntheticId(date: LocalDate, policyTable: DataFrame, previousPolicyTableRaw: Dataset[AudienceModelPolicyRecord], previousPolicyTableDate: LocalDate): DataFrame = {

    val storageCloud = lit(StorageCloud.withName(conf.storageCloud).id)

    // use current date's seed id as active id, should be replaced with other table later
    val activeIds = policyTable.select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud).distinct().cache
    val previousPolicyTable = previousPolicyTableRaw.drop("AdvertiserId", "IndustryCategoryId", "IsSensitive","NeedGraphExtension")
    // get retired sourceId
    val policyTableDayChange = ChronoUnit.DAYS.between(previousPolicyTableDate, date).toInt

    val inActiveIds = previousPolicyTable.filter('StorageCloud === storageCloud)
      .join(activeIds, Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
      .withColumn("ExpiredDays", 'ExpiredDays + lit(policyTableDayChange))

    val releasedIds = inActiveIds.filter('ExpiredDays > conf.expiredDays)

    val continueIds = previousPolicyTable.join(releasedIds, Seq("SyntheticId"), "left_anti")

    val retentionIds = inActiveIds.filter('ExpiredDays <= conf.expiredDays)
      .withColumn("IsActive", lit(false))
      .withColumn("Tag", lit(Tag.Retention.id))
      .withColumn("SampleWeight", lit(1.0))
    // get new sourceId
    val newIds = activeIds.join(previousPolicyTable.filter('StorageCloud === storageCloud)
      .select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
    val newIdsCount = newIds.count().toInt

    logger.info("newIdsCount: " + newIdsCount)

    val releasedIdsCount = releasedIds.count().toInt
    // get max SyntheticId from previous policy table
    val maxId = math.max(previousPolicyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int], previousPolicyTable.count().toInt + newIdsCount - releasedIdsCount)

    val currentActiveIds = policyTable
      .join(previousPolicyTable.filter('StorageCloud === storageCloud).select('SourceId, 'Source, 'CrossDeviceVendorId, 'SyntheticId, 'IsActive, 'StorageCloud, 'MappingId), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("Tag", when('IsActive, lit(Tag.Existing.id)).otherwise(lit(Tag.Recall.id)))
      .withColumn("IsActive", lit(true))
      .withColumn("SampleWeight", lit(1.0)) // todo optimize this with performance/monitoring
      .drop("NeedGraphExtension")
    logger.info("currentActiveIds: " + currentActiveIds.count())

    val otherSourcePreviousPolicyTable = previousPolicyTable.filter('StorageCloud =!= storageCloud).toDF()

    val updatedPolicyTable =
      if (newIdsCount > 0) {
        val syntheticIdPool = spark
          .range(1, maxId + 1)
          .toDF("SyntheticId")
          .select('SyntheticId.cast(IntegerType).as("SyntheticId"))
          .join(continueIds, Seq("SyntheticId"), "left_anti")
          .withColumn("order", row_number().over(Window.orderBy('SyntheticId.asc)))
          .where('order <= lit(newIdsCount))
          .select('SyntheticId, 'order)

        // get max SyntheticId over source type and CrossDeviceVendor from previous policy table

        val maxIdOverSourceNGraph = previousPolicyTable.groupBy('Source, 'CrossDeviceVendorId).agg(max('SyntheticId).as("MaxSyntheticId"))
          .join(previousPolicyTable.groupBy('Source, 'CrossDeviceVendorId).agg(count("*").alias("Count")), Seq("Source", "CrossDeviceVendorId"), "outer")
          .join(releasedIds.groupBy('Source, 'CrossDeviceVendorId).agg(count("*").alias("ReleasedCount")), Seq("Source", "CrossDeviceVendorId"), "outer")
          .join(newIds.groupBy('Source, 'CrossDeviceVendorId).agg(count("*").alias("NewCount")), Seq("Source", "CrossDeviceVendorId"), "right")
          .select('Source, 'CrossDeviceVendorId, greatest(coalesce('MaxSyntheticId, lit(1)), coalesce('Count, lit(0)) + 'NewCount - coalesce('ReleasedCount, lit(0))).cast(IntegerType).as("Value"))
          .as[SourceNGraphValue].collect()
        val mappingIdPool = maxIdOverSourceNGraph.map(e =>
            spark
              .range(1, e.Value + 1)
              .toDF("MappingId")
              .select('MappingId.cast(IntegerType).as("MappingId"))
              .withColumn("Source", lit(e.Source))
              .withColumn("CrossDeviceVendorId", lit(e.CrossDeviceVendorId)))
          .reduce(_ union _)
          .join(continueIds, Seq("Source", "CrossDeviceVendorId", "MappingId"), "left_anti")
          .withColumn("order", row_number().over(Window.partitionBy('Source, 'CrossDeviceVendorId).orderBy('MappingId.asc)))
          .join(newIds.groupBy('Source, 'CrossDeviceVendorId).agg(count("*").alias("NewCount")), Seq("Source", "CrossDeviceVendorId"))
          .where('order <= 'NewCount)
          .select('MappingId, 'Source, 'CrossDeviceVendorId, 'order)

        val updatedIds = newIds
          // assign available synthetic ids randomly to new source ids
          .withColumn("order", row_number().over(Window.orderBy(rand())))
          .join(syntheticIdPool, Seq("order"))
          .drop("order")
          // assign available mapping ids randomly over source and crossdevicevendorid to new source ids
          .withColumn("order", row_number().over(Window.partitionBy('Source, 'CrossDeviceVendorId).orderBy(rand())))
          .join(mappingIdPool, Seq("order", "Source", "CrossDeviceVendorId"), "left")
          .drop("order")
          .withColumn("MappingId", coalesce('MappingId, lit(-1))) // assign to -1 so we could re-verify in @see [[com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGenerator.evaluatePolicyTable]] in case any missing data
          .join(policyTable.drop("SyntheticId"), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
          .withColumn("ExpiredDays", lit(0))
          .withColumn("IsActive", lit(true))
          .withColumn("Tag", lit(Tag.New.id))
          .withColumn("SampleWeight", lit(1.0)).cache()
        updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)
      } else {
        retentionIds.unionByName(currentActiveIds)
      }

    val finalUpdatedPolicyTable = updatedPolicyTable.unionByName(otherSourcePreviousPolicyTable).cache()

    evaluatePolicyTable(finalUpdatedPolicyTable)
    finalUpdatedPolicyTable
  }

  private def evaluatePolicyTable(policyTable: DataFrame): Unit = {
    require(policyTable.count() == policyTable.select('SyntheticId).distinct().count(), "conflict synthetic ids")
    require(policyTable.count() == policyTable.select('Source, 'CrossDeviceVendorId, 'MappingId).distinct().count(), "conflict mapping ids")
    require(policyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int] <= 500000, "maximal synthetic id is 500000")
    require(policyTable.agg(max('MappingId)).collect()(0)(0).asInstanceOf[Int] < 65536 * 2, "maximal mapping id is 131071")
    require(policyTable.agg(min('MappingId)).collect()(0)(0).asInstanceOf[Int] >= 0, "minimal mapping id is 0")
  }

  def retrieveSourceData(date: LocalDate): DataFrame

  private def allocateSyntheticId(dateTime: LocalDateTime, policyTable: DataFrame): DataFrame = {
    val recentVersionOption = if (conf.seedMetaDataRecentVersion.isDefined) Some(LocalDateTime.parse(conf.seedMetaDataRecentVersion.get.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
    else availablePolicyTableVersions.find(_.isBefore(dateTime))

    if (!(recentVersionOption.isDefined) || conf.policyTableResetSyntheticId) {
      logger.info("Going to reset synthetic ids!!!")
      val updatedPolicyTable = policyTable
        .withColumn("SyntheticId", row_number().over(Window.orderBy(rand())))
        .withColumn("SampleWeight", lit(1.0))
        .withColumn("IsActive", lit(true))
        // TODO: update tag info from other signal tables (offline/online monitor, etc)
        .withColumn("Tag", lit(Tag.New.id))
        .withColumn("ExpiredDays", lit(0))
        .withColumn("MappingId", row_number().over(Window.partitionBy('Source, 'CrossDeviceVendorId).orderBy(rand())))
      updatedPolicyTable
    } else {
      logger.info("dateTime: " + dateTime.toLocalDate.toString)
      logger.info("recentVersion: " + recentVersionOption.get.toLocalDate.toString)
      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
        .readSinglePartition(recentVersionOption.get)(spark)
      updateSyntheticId(dateTime.toLocalDate, policyTable, previousPolicyTable, recentVersionOption.get.toLocalDate)
    }
  }

  protected def activeUserRatio(dateTime: LocalDateTime): Double = {
    val recentVersionOption = conf.seedMetaDataRecentVersion
      .map(v => LocalDateTime
        .parse(v.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS"))
        .toLocalDate
        .atStartOfDay())
      .orElse(availablePolicyTableVersions.find(_.isBefore(dateTime)))

    if (recentVersionOption.isEmpty || conf.policyTableResetSyntheticId) {
      conf.activeUserRatio
    } else {
      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
        .readSinglePartition(recentVersionOption.get)(spark).cache()
      previousPolicyTable
        .where('CrossDeviceVendorId === lit(CrossDeviceVendor.None.id))
        .agg(sum('ActiveSize) / sum('Size)).collect()(0).getDouble(0)
    }
  }
}

case class SourceNGraphValue(Source: Int, CrossDeviceVendorId: Int, Value: Int)
