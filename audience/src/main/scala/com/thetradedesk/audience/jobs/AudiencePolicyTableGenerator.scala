// ------------- OUTDATED: WILL DELETE -----------------------------
// package com.thetradedesk.audience.jobs
//
//import com.thetradedesk.audience.datasets.CrossDeviceVendor.{CrossDeviceVendor, IAV2Person}
//import com.thetradedesk.audience.datasets.Model.Model
//import com.thetradedesk.audience.datasets.SeedTagOperations.{dataSourceCheck, dataSourceTag}
//import com.thetradedesk.audience.datasets.{AggregatedSeedReadableDataset, _}
//import com.thetradedesk.audience.utils.Logger.Log
//import com.thetradedesk.audience.utils.{BitwiseOrAgg, S3Utils}
//import com.thetradedesk.audience._
//import com.thetradedesk.audience.jobs.AudiencePolicyTableGeneratorJob.prometheus
//import com.thetradedesk.audience.jobs.RSMPolicyTableGenerator.getBidImpUniqueTDIDs
//import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
//import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
//import com.thetradedesk.spark.TTDSparkContext.spark
//import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
//import com.thetradedesk.spark.datasets.core.AnnotatedSchemaBuilder
//import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
//import com.thetradedesk.spark.util.io.FSUtils
//import com.thetradedesk.spark.util.prometheus.PrometheusClient
//import org.apache.commons.lang3.exception.ExceptionUtils
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
//
//import java.time.format.DateTimeFormatter
//import java.time.temporal.ChronoUnit
//import java.time.{LocalDate, LocalDateTime, ZoneOffset}
//import scala.util.Random
//import java.sql.Timestamp
//
//object AudiencePolicyTableGeneratorJob {
//  val prometheus = new PrometheusClient("AudienceModelJob", "AudiencePolicyTableGeneratorJob")
//
//  object Config {
//    val model = Model.withName(config.getString("modelName", default = "RSM"))
//    val lookBack = config.getInt("lookBack", default = 3)
//  }
//
//  def main(args: Array[String]): Unit = {
//    runETLPipeline()
//    prometheus.pushMetrics()
//  }
//
//  def runETLPipeline(): Unit = {
//    Config.model match {
//      case Model.RSM =>
//        RSMPolicyTableGenerator.generatePolicyTable()
//      case Model.AEM =>
//        AEMPolicyTableGenerator.generatePolicyTable()
//      case _ => throw new Exception(s"unsupported Model[${Config.model}]")
//    }
//  }
//}
//
//
//abstract class AudiencePolicyTableGenerator(model: Model, prometheus: PrometheusClient) {
//
//  val userDownSampleHitPopulation = config.getInt(s"userDownSampleHitPopulation${model}", default = 100000)
//  val samplingFunction = shouldConsiderTDID3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))(_)
//  val arraySamplingFunction = shouldConsiderTDIDInArray3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))
//  val jobRunningTime = prometheus.createGauge(s"audience_policy_table_job_running_time", "AudiencePolicyTableGenerator running time", "model", "date")
//  val policyTableSize = prometheus.createGauge(s"audience_policy_table_size", "AudiencePolicyTableGenerator running time", "model", "date")
//
//
//  object Config {
//    // config to determine which cloud storage source to use
//    val storageCloud = StorageCloud.withName(config.getString("storageCloud", StorageCloud.AWS.toString)).id
//    // detect recent seed metadata path in airflow and pass to spark job
//    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
//    val seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
//    val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
//    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
//    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1"))
//    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", null)
//    val policyTableResetSyntheticId = config.getBoolean("policyTableResetSyntheticId", false)
//    // conversion data look back days
//    val conversionLookBack = config.getInt("conversionLookBack", 5)
//    val expiredDays = config.getInt("expiredDays", default = 7)
//    val policyTableLookBack = config.getInt("policyTableLookBack", default = 3)
//    val policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1"))
//    val policyS3Path = S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1"))
//    val maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30)
//    val bidImpressionRepartitionNum = config.getInt("bidImpressionRepartitionNum", 4096)
//    val seedRepartitionNum = config.getInt("seedRepartitionNum", 32)
//    val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1)
//    val graphUniqueCountKeepThreshold = config.getInt("graphUniqueCountKeepThreshold", 20)
//    val graphScoreThreshold = config.getDouble("graphScoreThreshold", 0.01)
//    val seedJobParallel = config.getInt("seedJobParallel", Runtime.getRuntime.availableProcessors())
//    val seedProcessLowerThreshold = config.getLong("seedProcessLowerThreshold", 2000)
//    val seedProcessUpperThreshold = config.getLong("seedProcessUpperThreshold", 100000000)
//    val seedExtendGraphUpperThreshold = config.getLong("seedExtendGraphUpperThreshold", 3000000)
//    val activeUserRatio = config.getDouble("activeUserRatio", 0.4)
//    val aemPixelLimit = config.getInt("aemPixelLimit", 5000)
//    var selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-mlplatform-us-east-1/configdata/prodTest/audience/other/AEM/selectedPixelTrackingTagIds/")
//    var useSelectedPixel = config.getBoolean("useSelectedPixel", false)
//    var campaignFlightStartingBufferInDays = config.getInt("campaignFlightStartingBufferInDays", 14)
//    var allRSMSeed = config.getBoolean("allRSMSeed", false)
//  }
//
//  private val policyTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)
//
//  private val availablePolicyTableVersions = S3Utils
//    .queryCurrentDataVersions(Config.policyS3Bucket, Config.policyS3Path)
//    .map(LocalDateTime.parse(_, policyTableDateFormatter))
//    .toSeq
//    .sortWith(_.isAfter(_))
//
//  def generatePolicyTable(): Unit = {
//
//    val start = System.currentTimeMillis()
//
//    val policyTable = retrieveSourceData(dateTime.toLocalDate)
//
//    val policyTableResult = allocateSyntheticId(dateTime, policyTable)
//
//    AudienceModelPolicyWritableDataset(model)
//      .writePartition(
//        policyTableResult.as[AudienceModelPolicyRecord],
//        dateTime,
//        saveMode = SaveMode.Overwrite
//      )
//    // disable it, moving it when generating embeddings
//    // updatePolicyCurrentVersion(dateTime)
//    policyTableSize.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(policyTableResult.count())
//    jobRunningTime.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
//  }
//
//  def getBidImpUniqueTDIDs(date: LocalDate) = {
//    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
//
//    val uniqueTDIDs = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(Config.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
//      .withColumnRenamed("UIID", "TDID")
//      .filter(samplingFunction('TDID))
//      .select('TDID)
//      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
//      .distinct()
//      .cache()
//
//    uniqueTDIDs
//  }
//
//  def readGraphData(date: LocalDate, crossDeviceVendor: CrossDeviceVendor)(implicit spark: SparkSession): DataFrame = {
//    val graphData = {
//      if (crossDeviceVendor == CrossDeviceVendor.IAV2Person) {
//        CrossDeviceGraphUtil
//          .readGraphData(date, LightCrossDeviceGraphDataset())
//          .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
//          .select('uiid.alias("TDID"), 'personId.alias("groupId"))
//      } else if (crossDeviceVendor == CrossDeviceVendor.IAV2Household) {
//        CrossDeviceGraphUtil
//          .readGraphData(date, LightCrossDeviceHouseholdGraphDataset())
//          .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
//          .select('uiid.alias("TDID"), 'householdID.alias("groupId"))
//      } else {
//        throw new UnsupportedOperationException(s"crossDeviceVendor ${crossDeviceVendor} is not supported")
//      }
//    }
//
//    if (dryRun) {
//      graphData.where(samplingFunction('groupId))
//    } else {
//      graphData
//    }
//  }
//
//  def generateGraphMapping(sourceGraph: DataFrame): DataFrame = {
//    val graph = sourceGraph
//      .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
//      .groupBy('groupId)
//      .agg(collect_set('uiid).alias("TDID"))
//      .where(size('TDID) > lit(1) && size('TDID) <= lit(Config.graphUniqueCountKeepThreshold)) // remove persons with too many individuals or only one TDID
//      .cache()
//
//    val sampledGraph = graph
//      .select('groupId, arraySamplingFunction('TDID).alias("co_TDIDs"))
//      .where(size('co_TDIDs) > lit(0))
//
//    val mapping = graph
//      .join(
//        sampledGraph,
//        Seq("groupId"),
//        "inner")
//      .select(explode('TDID).alias("TDID"), 'co_TDIDs)
//      .select('TDID, array_remove('co_TDIDs, 'TDID).alias("co_TDIDs"))
//      .where(size('co_TDIDs) > lit(0))
//
//    mapping
//  }
//
//  private def updatePolicyCurrentVersion(dateTime: LocalDateTime) = {
//    val versionContent = (availablePolicyTableVersions :+ dateTime)
//      .distinct
//      .sortWith(_.isAfter(_))
//      .take(Config.maxVersionsToKeep)
//      .map(_.format(policyTableDateFormatter))
//      .mkString("\n")
//    S3Utils.updateCurrentDataVersion(Config.policyS3Bucket, Config.policyS3Path, versionContent)
//  }
//
//  // todo assign synthetic id, weight, tag, and isActive
//
//  private def updateSyntheticId(date: LocalDate, policyTable: DataFrame, previousPolicyTable: Dataset[AudienceModelPolicyRecord], previousPolicyTableDate: LocalDate): DataFrame = {
//    // use current date's seed id as active id, should be replaced with other table later
//    val activeIds = policyTable.select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud).distinct().cache
//    // get retired sourceId
//    val policyTableDayChange = ChronoUnit.DAYS.between(previousPolicyTableDate, date).toInt
//
//    val inActiveIds = previousPolicyTable.filter('StorageCloud === Config.storageCloud)
//      .join(activeIds, Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
//      .withColumn("ExpiredDays", 'ExpiredDays + lit(policyTableDayChange))
//
//    val releasedIds = inActiveIds.filter('ExpiredDays > Config.expiredDays)
//
//    val retentionIds = inActiveIds.filter('ExpiredDays <= Config.expiredDays)
//      .withColumn("IsActive", lit(false))
//      .withColumn("Tag", lit(Tag.Retention.id))
//      .withColumn("SampleWeight", lit(1.0))
//
//    // get new sourceId
//    val newIds = activeIds.join(previousPolicyTable.filter('StorageCloud === Config.storageCloud)
//                                                  .select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
//    // get max SyntheticId from previous policy table
//    val maxId = previousPolicyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int]
//    // generate new synthetic Ids
//    val numNewIdNeeded = (newIds.count() - releasedIds.count()).toInt
//    // assign available syntheticids randomly to new sourceids
//    val allIdsAdded = Random.shuffle(releasedIds.select('SyntheticId).as[Int].collect().toSeq ++ Range.inclusive(maxId + 1, maxId + numNewIdNeeded, 1))
//    val getElementAtIndex = udf((index: Long) => allIdsAdded(index.toInt))
//    val updatedIds = newIds.withColumn("row_index", (row_number.over(Window.orderBy("SourceId", "CrossDeviceVendorId")) - 1).alias("row_index"))
//      .withColumn("SyntheticId", getElementAtIndex($"row_index")).drop("row_index")
//      .join(policyTable.drop("SyntheticId"), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
//      .withColumn("ExpiredDays", lit(0))
//      .withColumn("IsActive", lit(true))
//      .withColumn("Tag", lit(Tag.New.id))
//      .withColumn("SampleWeight", lit(1.0)).cache()
//
//    val currentActiveIds = policyTable
//      .join(previousPolicyTable.filter('StorageCloud === Config.storageCloud).select('SourceId, 'Source, 'CrossDeviceVendorId, 'SyntheticId, 'IsActive, 'StorageCloud), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
//      .withColumn("ExpiredDays", lit(0))
//      .withColumn("Tag", when('IsActive, lit(Tag.Existing.id)).otherwise(lit(Tag.Recall.id)))
//      .withColumn("IsActive", lit(true))
//      .withColumn("SampleWeight", lit(1.0)) // todo optimize this with performance/monitoring
//
//    val otherSourcePreviousPolicyTable = previousPolicyTable.filter('StorageCloud =!= Config.storageCloud).toDF()
//    val updatedPolicyTable = updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)
//    val finalUpdatedPolicyTable = updatedPolicyTable.unionByName(otherSourcePreviousPolicyTable)
//
//    finalUpdatedPolicyTable
//  }
//
//  def retrieveSourceData(date: LocalDate): DataFrame
//
//  private def allocateSyntheticId(dateTime: LocalDateTime, policyTable: DataFrame): DataFrame = {
//    val recentVersionOption = if (Config.seedMetaDataRecentVersion != null) Some(LocalDateTime.parse(Config.seedMetaDataRecentVersion.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
//    else availablePolicyTableVersions.find(_.isBefore(dateTime))
//
//    if (!(recentVersionOption.isDefined) || Config.policyTableResetSyntheticId) {
//      val updatedPolicyTable = policyTable
//        .withColumn("SyntheticId", row_number().over(Window.orderBy(rand())))
//        .withColumn("SampleWeight", lit(1.0))
//        .withColumn("IsActive", lit(true))
//        // TODO: update tag info from other signal tables (offline/online monitor, etc)
//        .withColumn("Tag", lit(Tag.New.id))
//        .withColumn("ExpiredDays", lit(0))
//      updatedPolicyTable
//    } else {
//      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
//        .readSinglePartition(recentVersionOption.get)(spark)
//      updateSyntheticId(dateTime.toLocalDate, policyTable, previousPolicyTable, recentVersionOption.get.toLocalDate)
//    }
//  }
//
//  protected def activeUserRatio(dateTime: LocalDateTime): Double = {
//    val recentVersionOption = if (Config.seedMetaDataRecentVersion != null) Some(LocalDateTime.parse(Config.seedMetaDataRecentVersion.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
//    else availablePolicyTableVersions.find(_.isBefore(dateTime))
//
//    if (recentVersionOption.isEmpty || Config.policyTableResetSyntheticId) {
//      Config.activeUserRatio
//    } else {
//      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
//        .readSinglePartition(recentVersionOption.get)(spark).cache()
//      previousPolicyTable
//        .where('CrossDeviceVendorId === lit(CrossDeviceVendor.None.id))
//        .agg(sum('ActiveSize) / sum('Size)).collect()(0).getDouble(0)
//    }
//  }
//}
//
//object RSMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.RSM, prometheus: PrometheusClient) {
//
//  val bitwiseOrAgg = udaf(BitwiseOrAgg)
//  val rsmSeedProcessCount = prometheus.createCounter("rsm_policy_table_job_seed_process_count", "RSM policy table job seed process record", "seedId", "success")
//
//  override def retrieveSourceData(date: LocalDate): DataFrame = {
//
//    val recentVersion =
//      if (Config.seedMetaDataRecentVersion != null) Config.seedMetaDataRecentVersion
//      else S3Utils.queryCurrentDataVersion(Config.seedMetadataS3Bucket, Config.seedMetadataS3Path)
//
//    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + "/" + Config.seedMetadataS3Path + "/" + recentVersion
//
//    val uniqueTDIDs = getBidImpUniqueTDIDs(date)
//
//    val personGraph = readGraphData(date, CrossDeviceVendor.IAV2Person)(spark).cache()
//
//    val sampledPersonGraph = personGraph
//      .select('TDID, 'groupId)
//      .where(samplingFunction('TDID))
//      .withColumnRenamed("groupId", "personId")
//      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
//
//    val householdGraph = readGraphData(date, CrossDeviceVendor.IAV2Household)(spark).cache()
//
//    val sampledHouseholdGraph = householdGraph
//      .select('TDID, 'groupId)
//      .where(samplingFunction('TDID))
//      .withColumnRenamed("groupId", "householdId")
//      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
//
//    val sampledGraph = sampledPersonGraph
//      .join(sampledHouseholdGraph, Seq("TDID"), "outer")
//      .cache()
//
//    val seedMeta =
//    if (!Config.allRSMSeed) {
//      val startDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date.plusDays(Config.campaignFlightStartingBufferInDays))
//      val endDateTimeStr = DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00").format(date)
//
//      val campaignFlight = CampaignFlightDataSet()
//        .readPartition(date)
//        .where('IsDeleted === false
//          && 'StartDateInclusiveUTC.leq(startDateTimeStr)
//          && ('EndDateExclusiveUTC.isNull || 'EndDateExclusiveUTC.gt(endDateTimeStr)))
//        .select('CampaignId)
//        .distinct()
//
//      val campaignSeed = CampaignSeedDataset()
//        .readPartition(date)
//
//      val optInSeed = campaignSeed
//        .join(campaignFlight, Seq("CampaignId"), "inner")
//        .select('SeedId)
//        .distinct()
//
//      spark.read.parquet(seedDataFullPath)
//        .join(optInSeed, Seq("SeedId"), "inner")
//        .select('SeedId, 'TargetingDataId, coalesce('Count, lit(-1)).alias("Count"), 'Path)
//        .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
//        .cache()
//    } else {
//      spark.read.parquet(seedDataFullPath)
//        .select('SeedId, 'TargetingDataId, coalesce('Count, lit(-1)).alias("Count"), 'Path)
//        .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
//        .cache()
//    }
//
//    val policyMetaTable = seedMeta
//      .as[SeedDetail]
//      .collect()
//
//    // TODO support seed data cache
//
//    policyMetaTable
//      .filter(e => e.Count < Config.seedProcessLowerThreshold || e.Count > Config.seedProcessUpperThreshold)
//      .foreach(e => rsmSeedProcessCount.labels(e.SeedId, "Cutoff").inc())
//
//    val largeSeedData = handleLargeSeedData(policyMetaTable.filter(e => e.Count > Config.seedExtendGraphUpperThreshold && e.Count <= Config.seedProcessUpperThreshold).map(i => i.Path))
//    val smallSeedData = handleSmallSeedData(personGraph, householdGraph, policyMetaTable.filter(e => e.Count >= Config.seedProcessLowerThreshold && e.Count <= Config.seedExtendGraphUpperThreshold).map(i => i.Path))
//
//    val allSeedData = largeSeedData
//      .union(smallSeedData._1)
//      .groupBy('TDID)
//      .agg(collect_list('SeedIds).alias("SeedIds"))
//      .select('TDID, flatten('SeedIds).alias("SeedIds"))
//      .join(smallSeedData._2.select('TDID, 'SeedIds.alias("PersonGraphSeedIds")), Seq("TDID"), "outer")
//      .join(smallSeedData._3.select('TDID, 'SeedIds.alias("HouseholdGraphSeedIds")), Seq("TDID"), "outer")
//
//    val allFinalSeedData =
//      allSeedData.join(uniqueTDIDs, Seq("TDID"), "inner")
//        .join(sampledGraph, Seq("TDID"), "left")
//        .select('TDID,
//          coalesce('SeedIds, typedLit(Array.empty[String])).alias("SeedIds"),
//          coalesce('PersonGraphSeedIds, typedLit(Array.empty[String])).alias("PersonGraphSeedIds"),
//          coalesce('HouseholdGraphSeedIds, typedLit(Array.empty[String])).alias("HouseholdGraphSeedIds"),
//          coalesce('personId, 'TDID).alias("personId"),
//          coalesce('householdId, 'TDID).alias("householdId"))
//        .select('TDID, 'SeedIds,
//          array_except('PersonGraphSeedIds, 'SeedIds).alias("PersonGraphSeedIds"),
//          array_except('HouseholdGraphSeedIds, 'SeedIds).alias("HouseholdGraphSeedIds"),
//          'personId,
//          'householdId
//        )
//        .as[AggregatedSeedRecord]
//
//    AggregatedSeedWritableDataset()
//      .writePartition(allFinalSeedData,
//        date,
//        saveMode = SaveMode.Overwrite)
//
//    // make sure dataset is written successfully to s3
//    var checkCount = 0
//    val successFile = AggregatedSeedReadableDataset().DatePartitionedPath(Some(date)) + "/_SUCCESS"
//    while (checkCount < 10 && !FSUtils.fileExists(successFile)(spark)) {
//      checkCount += 1
//      Thread.sleep(1000 * checkCount)
//    }
//    if (checkCount == 10) {
//      throw new Exception(s"final seed data failed to sync, success file ${successFile}")
//    }
//
//    val finalSeedData = AggregatedSeedReadableDataset().readPartition(date)(spark)
//
//    val nonGraphCount = rawSeedCount(finalSeedData)
//    val personGraphCount =
//      extendGraphSeedCount(nonGraphCount, finalSeedData, CrossDeviceVendor.IAV2Person)
//    val householdGraphCount =
//      extendGraphSeedCount(nonGraphCount, finalSeedData, CrossDeviceVendor.IAV2Household)
//
//    val policyTable =
//      nonGraphCount
//        .union(personGraphCount)
//        .union(householdGraphCount)
//        .withColumnRenamed("count", "ActiveSize")
//        .join(seedMeta, Seq("SeedId"), "inner")
//        .select(('ActiveSize * (userDownSampleBasePopulation / userDownSampleHitPopulation)).alias("ActiveSize"), 'CrossDeviceVendorId, 'SeedId.alias("SourceId"), 'Count.alias("Size"), 'TargetingDataId)
//        .withColumn("Source", lit(DataSource.Seed.id))
//        .withColumn("GoalType", lit(GoalType.Relevance.id))
//        .withColumn("StorageCloud", lit(Config.storageCloud))
//        .cache()
//
//    policyTable
//  }
//
//  private def rawSeedCount(allFinalSeedData: Dataset[AggregatedSeedRecord]): DataFrame = {
//    allFinalSeedData
//      .select(explode('SeedIds).alias("SeedId"))
//      .groupBy('SeedId)
//      .count()
//      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
//      .cache()
//  }
//
//  private def extendGraphSeedCount(nonGraphCount: DataFrame, allFinalSeedData: Dataset[AggregatedSeedRecord], crossDeviceVendor: CrossDeviceVendor): DataFrame = {
//    allFinalSeedData
//      .select(explode(if (crossDeviceVendor == IAV2Person) 'PersonGraphSeedIds else 'HouseholdGraphSeedIds).alias("SeedId"))
//      .groupBy('SeedId)
//      .count()
//      .join(nonGraphCount.select('SeedId, 'count.alias("RawCount"))
//        , Seq("SeedId"), "inner")
//      .select('SeedId, ('count + 'RawCount).alias("count"))
//      .withColumn("CrossDeviceVendorId", lit(crossDeviceVendor.id))
//  }
//
//  private def retrieveSeedData(paths: Array[String], isSample: Boolean): (DataFrame, DataFrame) = {
//    val basePath = "s3://" + Config.seedRawDataS3Bucket + "/" + Config.seedRawDataS3Path
//    val seedDataSet =
//      if (isSample) {
//        spark.read.option("basePath", basePath)
//          .schema(AnnotatedSchemaBuilder.schema[SeedRecord])
//          .parquet(paths: _*)
//          .where(samplingFunction('UserId))
//          .select('UserId.alias("TDID"), 'SeedId)
//      } else {
//        spark.read.option("basePath", basePath)
//          .schema(AnnotatedSchemaBuilder.schema[SeedRecord])
//          .parquet(paths: _*)
//          .select('UserId.alias("TDID"), 'SeedId)
//      }
//    val TDID2Seeds = seedDataSet
//      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
//      .groupBy('TDID)
//      .agg(collect_set('SeedId).alias("SeedIds"))
//
//    (TDID2Seeds, seedDataSet)
//  }
//
//  private val seedDataSchema = new StructType()
//    .add("TDID", StringType)
//    .add("SeedIds", ArrayType(StringType))
//
//  private def handleLargeSeedData(policyMetaTable: Array[String]): DataFrame = {
//    if (policyMetaTable.isEmpty) {
//      return spark.createDataFrame(spark.sparkContext
//        .emptyRDD[Row], seedDataSchema)
//    }
//    val tempPolicyMetaTable = if (dryRun) policyMetaTable.take(10) else policyMetaTable
//    retrieveSeedData(tempPolicyMetaTable, isSample = true)._1
//  }
//
//  private def handleSmallSeedData(personGraph: DataFrame, householdGraph: DataFrame, policyMetaTable: Array[String]): (DataFrame, DataFrame, DataFrame) = {
//    if (policyMetaTable.isEmpty) {
//      return (spark.createDataFrame(spark.sparkContext
//        .emptyRDD[Row], seedDataSchema),
//        spark.createDataFrame(spark.sparkContext
//          .emptyRDD[Row], seedDataSchema),
//        spark.createDataFrame(spark.sparkContext
//          .emptyRDD[Row], seedDataSchema))
//    }
//
//    val tempPolicyMetaTable = if (dryRun) policyMetaTable.take(10) else policyMetaTable
//
//    val seedData = retrieveSeedData(tempPolicyMetaTable, isSample = false)
//
//    val personGraphSeedData = readGraphDataAndExtendSeed(seedData._2, personGraph)(spark)
//    val householdGraphSeedData = readGraphDataAndExtendSeed(seedData._2, householdGraph)(spark)
//
//    val sampledSeedData = seedData._1
//      .where(samplingFunction('TDID))
//
//    (sampledSeedData, personGraphSeedData, householdGraphSeedData)
//  }
//
//  private def readGraphDataAndExtendSeed(seedData: DataFrame, graph: DataFrame)(implicit spark: SparkSession): DataFrame = {
//    val seedWithGraph = graph.join(seedData, Seq("TDID"), "inner")
//
//    val groupIdToSeedIds = seedWithGraph
//      .groupBy('groupId)
//      .agg(collect_set('SeedId).alias("SeedIds"))
//
//    val graphGroup = graph
//      .groupBy('groupId)
//      .agg(collect_set('TDID).alias("TDIDs"))
//      .select('groupId, arraySamplingFunction('TDIDs).alias("TDIDs"))
//      .where(size('TDIDs) > lit(0))
//
//    val TDID2Seeds = groupIdToSeedIds
//      .join(graphGroup, Seq("groupId"), "inner")
//      .select(explode('TDIDs).alias("TDID"), 'SeedIds)
//
//    TDID2Seeds
//  }
//
//  private def extendSeedWithGraphs(seedData: DataFrame, graph: DataFrame, crossDeviceVendor: CrossDeviceVendor): DataFrame = {
//    val seedWithGraph = graph
//      .join(seedData, Seq("TDID"), "inner")
//      .select(explode('co_TDIDs).alias("TDID"))
//      .join(seedData, Seq("TDID"), "leftanti")
//      .withColumn("tag", dataSourceTag(crossDeviceVendor))
//
//    seedWithGraph
//  }
//}
//
//object AEMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.AEM, prometheus: PrometheusClient) {
//  val conversionSamplingFunction = shouldConsiderTDID3(config.getInt("userDownSampleHitPopulationAEMConversion", default = 1000000), config.getString("userDownSampleSaltAEMConversion", "KUQ3@F"))(_)
//
//  override def retrieveSourceData(date: LocalDate): DataFrame = {
//    retrieveConversionData(date: LocalDate)
//  }
//
//  private def retrieveActiveCampaignConversionTrackerTagIds(): DataFrame = {
//    // prepare dataset
//    val Campaign = CampaignDataSet().readLatestPartition()
//    val AdGroup = AdGroupDataSet().readLatestPartition()
//    val Partner = PartnerDataSet().readLatestPartition()
//    val CampConv = CampaignConversionReportingColumnDataset().readLatestPartition()
//
//    // calculate active campaign time range
//    val now = LocalDateTime.now(ZoneOffset.UTC)
//    val endDateThreshold = now.minusHours(48)
//    val startDateThreshold = now.plusHours(48)
//    val startTimestamp = Timestamp.valueOf(startDateThreshold)
//    val endTimestamp = Timestamp.valueOf(endDateThreshold)
//
//    val activeCampaignConversionTrackerTagIds = AdGroup
//      .join(Campaign, AdGroup("CampaignId") === Campaign("CampaignId"))
//      .join(Partner, Campaign("PartnerId") === Partner("PartnerId"))
//      .join(CampConv, Campaign("CampaignId") === CampConv("CampaignId"))
//      .filter(
//        AdGroup("IsEnabled") === 1 &&
//          Campaign("StartDate").lt(startTimestamp) &&
//          (Campaign("EndDate").isNull || Campaign("EndDate").gt(endTimestamp)) &&
//          Partner("SpendDisabled") === 0
//      )
//      .select(CampConv("TrackingTagId")).distinct()
//
//    activeCampaignConversionTrackerTagIds
//  }
//
//  private def retrieveConversionData(date: LocalDate): DataFrame = {
//    val uniqueTDIDsFromBidImp = getBidImpUniqueTDIDs(date)
//
//    // conversion
//    val activeConversionTrackerTagId = retrieveActiveCampaignConversionTrackerTagIds();
//
//    var conversionDataset = ConversionDataset(defaultCloudProvider)
//      .readRange(date.minusDays(Config.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
//      .select('TDID, 'TrackingTagId)
//      .filter(samplingFunction('TDID))
//
//    conversionDataset.cache();
//
//    val trackingTagDataset = LightTrackingTagDataset().readPartition(date)
//      .select("TrackingTagId", "TargetingDataId")
//
//    if (Config.useSelectedPixel) {
//      val selectedTrackingTagIds = spark.read.parquet(Config.selectedPixelsConfigPath)
//        .join(trackingTagDataset, "TargetingDataId").select("TrackingTagId")
//
//      conversionDataset =
//        conversionDataset.join(selectedTrackingTagIds, "TrackingTagId")
//          .join(activeConversionTrackerTagId, "TrackingTagId")
//
//    } else {
//      conversionDataset =
//        conversionDataset.join(activeConversionTrackerTagId, "TrackingTagId")
//    }
//
//    val conversionSize = conversionDataset
//      .groupBy('TrackingTagId)
//      .agg(
//        countDistinct('TDID)
//          .alias("Size"))
//
//    val conversionActiveSize = conversionDataset.join(uniqueTDIDsFromBidImp, "TDID")
//      .groupBy('TrackingTagId)
//      .agg(
//        countDistinct('TDID)
//          .alias("ActiveSize"))
//      .select(('ActiveSize * (userDownSampleBasePopulation / userDownSampleHitPopulation)).alias("ActiveSize"), 'TrackingTagId)
//
//    var conversionFinal: DataFrame = null
//
//    if (Config.useSelectedPixel) {
//      conversionFinal = conversionSize
//        .join(trackingTagDataset, "TrackingTagId")
//        .join(conversionActiveSize, "TrackingTagId")
//    } else {
//      conversionFinal = conversionSize
//        .join(conversionActiveSize, "TrackingTagId")
//        .join(trackingTagDataset, "TrackingTagId")
//        .orderBy(desc("ActiveSize"))
//        .limit(Config.aemPixelLimit)
//    }
//
//    val policyTable = conversionFinal
//      .withColumn("Source", lit(DataSource.Conversion.id))
//      .withColumn("GoalType", lit(GoalType.CPA.id))
//      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
//      .withColumnRenamed("TrackingTagId", "SourceId")
//
//
//    policyTable
//  }
//}
