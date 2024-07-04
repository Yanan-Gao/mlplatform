package com.thetradedesk.audience.jobs.policytable

import com.thetradedesk.audience._
import com.thetradedesk.audience.datasets.CrossDeviceVendor.{CrossDeviceVendor, IAV2Person}
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets.SeedTagOperations.dataSourceTag
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.jobs.policytable.AudiencePolicyTableGeneratorJob.prometheus
import com.thetradedesk.audience.utils.{BitwiseOrAgg, S3Utils}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.AnnotatedSchemaBuilder
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.util.Random

abstract class AudiencePolicyTableGenerator(model: Model, prometheus: PrometheusClient) {

  val userDownSampleHitPopulation = config.getInt(s"userDownSampleHitPopulation${model}", default = 100000)
  val samplingFunction = shouldConsiderTDID3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))(_)
  val arraySamplingFunction = shouldConsiderTDIDInArray3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))
  val jobRunningTime = prometheus.createGauge(s"audience_policy_table_job_running_time", "AudiencePolicyTableGenerator running time", "model", "date")
  val policyTableSize = prometheus.createGauge(s"audience_policy_table_size", "AudiencePolicyTableGenerator running time", "model", "date")


  object Config {
    // config to determine which cloud storage source to use
    val storageCloud = StorageCloud.withName(config.getString("storageCloud", StorageCloud.AWS.toString)).id
    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
    val seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
    val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1"))
    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", null)
    val policyTableResetSyntheticId = config.getBoolean("policyTableResetSyntheticId", false)
    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 5)
    val expiredDays = config.getInt("expiredDays", default = 7)
    val policyTableLookBack = config.getInt("policyTableLookBack", default = 3)
    val policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val policyS3Path = S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1"))
    val maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30)
    val bidImpressionRepartitionNum = config.getInt("bidImpressionRepartitionNum", 4096)
    val seedRepartitionNum = config.getInt("seedRepartitionNum", 32)
    val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1)
    val graphUniqueCountKeepThreshold = config.getInt("graphUniqueCountKeepThreshold", 20)
    val graphScoreThreshold = config.getDouble("graphScoreThreshold", 0.01)
    val seedJobParallel = config.getInt("seedJobParallel", Runtime.getRuntime.availableProcessors())
    val seedProcessLowerThreshold = config.getLong("seedProcessLowerThreshold", 2000)
    val seedProcessUpperThreshold = config.getLong("seedProcessUpperThreshold", 100000000)
    val seedExtendGraphUpperThreshold = config.getLong("seedExtendGraphUpperThreshold", 3000000)
    val activeUserRatio = config.getDouble("activeUserRatio", 0.4)
    val aemPixelLimit = config.getInt("aemPixelLimit", 5000)
    var selectedPixelsConfigPath = config.getString("selectedPixelsConfigPath", "s3a://thetradedesk-mlplatform-us-east-1/configdata/prodTest/audience/other/AEM/selectedPixelTrackingTagIds/")
    var useSelectedPixel = config.getBoolean("useSelectedPixel", false)
    var campaignFlightStartingBufferInDays = config.getInt("campaignFlightStartingBufferInDays", 14)
    var allRSMSeed = config.getBoolean("allRSMSeed", false)
  }

  private val policyTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)

  private val availablePolicyTableVersions = S3Utils
    .queryCurrentDataVersions(Config.policyS3Bucket, Config.policyS3Path)
    .map(LocalDateTime.parse(_, policyTableDateFormatter))
    .toSeq
    .sortWith(_.isAfter(_))

  def generatePolicyTable(): Unit = {

    val start = System.currentTimeMillis()

    val policyTable = retrieveSourceData(dateTime.toLocalDate)

    val policyTableResult = allocateSyntheticId(dateTime, policyTable)

    AudienceModelPolicyWritableDataset(model)
      .writePartition(
        policyTableResult.as[AudienceModelPolicyRecord],
        dateTime,
        saveMode = SaveMode.Overwrite
      )

    policyTableSize.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(policyTableResult.count())
    jobRunningTime.labels(model.toString.toLowerCase, dateTime.toLocalDate.toString).set(System.currentTimeMillis() - start)
  }

  def getBidImpUniqueTDIDs(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val uniqueTDIDs = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(Config.bidImpressionLookBack), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      .filter(samplingFunction('TDID))
      .select('TDID)
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
      .distinct()
      .cache()

    uniqueTDIDs
  }

  def readGraphData(date: LocalDate, crossDeviceVendor: CrossDeviceVendor)(implicit spark: SparkSession): DataFrame = {
    val graphData = {
      if (crossDeviceVendor == CrossDeviceVendor.IAV2Person) {
        CrossDeviceGraphUtil
          .readGraphData(date, LightCrossDeviceGraphDataset())
          .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
          .select('uiid.alias("TDID"), 'personId.alias("groupId"))
      } else if (crossDeviceVendor == CrossDeviceVendor.IAV2Household) {
        CrossDeviceGraphUtil
          .readGraphData(date, LightCrossDeviceHouseholdGraphDataset())
          .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
          .select('uiid.alias("TDID"), 'householdID.alias("groupId"))
      } else {
        throw new UnsupportedOperationException(s"crossDeviceVendor ${crossDeviceVendor} is not supported")
      }
    }

    if (dryRun) {
      graphData.where(samplingFunction('groupId))
    } else {
      graphData
    }
  }

  def generateGraphMapping(sourceGraph: DataFrame): DataFrame = {
    val graph = sourceGraph
      .where(shouldTrackTDID('uiid) && 'score > lit(Config.graphScoreThreshold))
      .groupBy('groupId)
      .agg(collect_set('uiid).alias("TDID"))
      .where(size('TDID) > lit(1) && size('TDID) <= lit(Config.graphUniqueCountKeepThreshold)) // remove persons with too many individuals or only one TDID
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

  private def updateSyntheticId(date: LocalDate, policyTable: DataFrame, previousPolicyTable: Dataset[AudienceModelPolicyRecord], previousPolicyTableDate: LocalDate): DataFrame = {
    // use current date's seed id as active id, should be replaced with other table later
    val activeIds = policyTable.select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud).distinct().cache
    // get retired sourceId
    val policyTableDayChange = ChronoUnit.DAYS.between(previousPolicyTableDate, date).toInt

    val inActiveIds = previousPolicyTable.filter('StorageCloud === Config.storageCloud)
      .join(activeIds, Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
      .withColumn("ExpiredDays", 'ExpiredDays + lit(policyTableDayChange))

    val releasedIds = inActiveIds.filter('ExpiredDays > Config.expiredDays)

    val retentionIds = inActiveIds.filter('ExpiredDays <= Config.expiredDays)
      .withColumn("IsActive", lit(false))
      .withColumn("Tag", lit(Tag.Retention.id))
      .withColumn("SampleWeight", lit(1.0))

    // get new sourceId
    val newIds = activeIds.join(previousPolicyTable.filter('StorageCloud === Config.storageCloud)
                                  .select('SourceId, 'Source, 'CrossDeviceVendorId, 'StorageCloud), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "left_anti")
    // get max SyntheticId from previous policy table
    val maxId = previousPolicyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int]
    // generate new synthetic Ids
    val numNewIdNeeded = (newIds.count() - releasedIds.count()).toInt
    // assign available syntheticids randomly to new sourceids
    val allIdsAdded = Random.shuffle(releasedIds.select('SyntheticId).as[Int].collect().toSeq ++ Range.inclusive(maxId + 1, maxId + numNewIdNeeded, 1))
    val getElementAtIndex = udf((index: Long) => allIdsAdded(index.toInt))
    val updatedIds = newIds.withColumn("row_index", (row_number.over(Window.orderBy("SourceId", "CrossDeviceVendorId")) - 1).alias("row_index"))
      .withColumn("SyntheticId", getElementAtIndex($"row_index")).drop("row_index")
      .join(policyTable.drop("SyntheticId"), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("IsActive", lit(true))
      .withColumn("Tag", lit(Tag.New.id))
      .withColumn("SampleWeight", lit(1.0)).cache()

    val currentActiveIds = policyTable
      .join(previousPolicyTable.filter('StorageCloud === Config.storageCloud).select('SourceId, 'Source, 'CrossDeviceVendorId, 'SyntheticId, 'IsActive, 'StorageCloud), Seq("SourceId", "Source", "CrossDeviceVendorId", "StorageCloud"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("Tag", when('IsActive, lit(Tag.Existing.id)).otherwise(lit(Tag.Recall.id)))
      .withColumn("IsActive", lit(true))
      .withColumn("SampleWeight", lit(1.0)) // todo optimize this with performance/monitoring

    val otherSourcePreviousPolicyTable = previousPolicyTable.filter('StorageCloud =!= Config.storageCloud).toDF()
    val updatedPolicyTable = updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)
    val finalUpdatedPolicyTable = updatedPolicyTable.unionByName(otherSourcePreviousPolicyTable)

    finalUpdatedPolicyTable
  }

  def retrieveSourceData(date: LocalDate): DataFrame

  private def allocateSyntheticId(dateTime: LocalDateTime, policyTable: DataFrame): DataFrame = {
    val recentVersionOption = if (Config.seedMetaDataRecentVersion != null) Some(LocalDateTime.parse(Config.seedMetaDataRecentVersion.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
    else availablePolicyTableVersions.find(_.isBefore(dateTime))

    if (!(recentVersionOption.isDefined) || Config.policyTableResetSyntheticId) {
      val updatedPolicyTable = policyTable
        .withColumn("SyntheticId", row_number().over(Window.orderBy(rand())))
        .withColumn("SampleWeight", lit(1.0))
        .withColumn("IsActive", lit(true))
        // TODO: update tag info from other signal tables (offline/online monitor, etc)
        .withColumn("Tag", lit(Tag.New.id))
        .withColumn("ExpiredDays", lit(0))
      updatedPolicyTable
    } else {
      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
        .readSinglePartition(recentVersionOption.get)(spark)
      updateSyntheticId(dateTime.toLocalDate, policyTable, previousPolicyTable, recentVersionOption.get.toLocalDate)
    }
  }

  protected def activeUserRatio(dateTime: LocalDateTime): Double = {
    val recentVersionOption = if (Config.seedMetaDataRecentVersion != null) Some(LocalDateTime.parse(Config.seedMetaDataRecentVersion.split("=")(1), DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS")).toLocalDate.atStartOfDay())
    else availablePolicyTableVersions.find(_.isBefore(dateTime))

    if (recentVersionOption.isEmpty || Config.policyTableResetSyntheticId) {
      Config.activeUserRatio
    } else {
      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
        .readSinglePartition(recentVersionOption.get)(spark).cache()
      previousPolicyTable
        .where('CrossDeviceVendorId === lit(CrossDeviceVendor.None.id))
        .agg(sum('ActiveSize) / sum('Size)).collect()(0).getDouble(0)
    }
  }
}
