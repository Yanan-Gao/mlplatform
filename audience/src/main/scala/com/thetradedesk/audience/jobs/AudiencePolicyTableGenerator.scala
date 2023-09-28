package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.CrossDeviceVendor.CrossDeviceVendor
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets.SeedTagOperations.{dataSourceCheck, dataSourceTag}
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.utils.Logger.Log
import com.thetradedesk.audience.utils.{BitwiseOrAgg, S3Utils}
import com.thetradedesk.audience._
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.util.Random

object AudiencePolicyTableGeneratorJob {
  object Config {
    val model = Model.withName(config.getString("modelName", default = "RSM"))
    val lookBack = config.getInt("lookBack", default = 3)
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {
    Config.model match {
      case Model.RSM =>
        RSMPolicyTableGenerator.generatePolicyTable()
      case Model.AEM =>
        AEMPolicyTableGenerator.generatePolicyTable()
      case _ => throw new Exception(s"unsupported Model[${Config.model}]")
    }
  }
}


abstract class AudiencePolicyTableGenerator(model: Model) {

  val userDownSampleHitPopulation = config.getInt(s"userDownSampleHitPopulation${model}", default = 100000)
  val samplingFunction = shouldConsiderTDID3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))(_)
  val arraySamplingFunction = shouldConsiderTDIDInArray3(userDownSampleHitPopulation, config.getStringRequired(s"saltToSampleUser${model}"))

  object Config {
    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
    val seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
    val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1/SeedId="))
    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", null)
    val policyTableResetSyntheticId = config.getBoolean("policyTableResetSyntheticId", false)
    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 1)
    val expiredDays = config.getInt("expiredDays", default = 7)
    val policyTableLookBack = config.getInt("policyTableLookBack", default = 3)
    val policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val policyS3Path = S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1"))
    val maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30)
    val bidImpressionRepartitionNum = config.getInt("bidImpressionRepartitionNum", 1024)
    val seedRepartitionNum = config.getInt("seedRepartitionNum", 32)
    val bidImpressionLookBack = config.getInt("bidImpressionLookBack", 1)
    val graphUniqueCountKeepThreshold = config.getInt("graphUniqueCountKeepThreshold", 20)
    val graphScoreThreshold = config.getDouble("graphScoreThreshold", 0.01)
    val seedJobParallel = config.getInt("seedJobParallel", Runtime.getRuntime.availableProcessors())
    val seedProcessLowerThreshold = config.getLong("seedProcessLowerThreshold", 2000)
    val seedProcessUpperThreshold = config.getLong("seedProcessUpperThreshold", 100000000)
    val seedExtendGraphUpperThreshold = config.getLong("seedExtendGraphUpperThreshold", 3000000)
  }

  private val policyTableDateFormatter = DateTimeFormatter.ofPattern(audienceVersionDateFormat)

  private val availablePolicyTableVersions = S3Utils
    .queryCurrentDataVersions(Config.policyS3Bucket, Config.policyS3Path)
    .map(LocalDateTime.parse(_, policyTableDateFormatter))
    .toSeq
    .sortWith(_.isAfter(_))

  def generatePolicyTable(): Unit = {
    val policyTable = retrieveSourceData(dateTime.toLocalDate)

    val policyTableResult = allocateSyntheticId(dateTime, policyTable)

    AudienceModelPolicyWritableDataset(model)
      .writePartition(
        policyTableResult.as[AudienceModelPolicyRecord],
        dateTime,
        saveMode = SaveMode.Overwrite
      )
    // disable it, moving it when generating embeddings
    // updatePolicyCurrentVersion(dateTime)
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
    if (crossDeviceVendor == CrossDeviceVendor.IAV2Person) {
      CrossDeviceGraphUtil
        .readGraphData(date, LightCrossDeviceGraphDataset())
        .withColumnRenamed("personId", "groupId")
    } else if (crossDeviceVendor == CrossDeviceVendor.IAV2Household) {
      CrossDeviceGraphUtil
        .readGraphData(date, LightCrossDeviceHouseholdGraphDataset())
        .withColumnRenamed("householdID", "groupId")
    } else {
      throw new UnsupportedOperationException(s"crossDeviceVendor ${crossDeviceVendor} is not supported")
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

  private def updatePolicyCurrentVersion(dateTime: LocalDateTime) = {
    val versionContent = (availablePolicyTableVersions :+ dateTime)
      .distinct
      .sortWith(_.isAfter(_))
      .take(Config.maxVersionsToKeep)
      .map(_.format(policyTableDateFormatter))
      .mkString("\n")
    S3Utils.updateCurrentDataVersion(Config.policyS3Bucket, Config.policyS3Path, versionContent)
  }

  // todo assign synthetic id, weight, tag, and isActive

  private def updateSyntheticId(date: LocalDate, policyTable: DataFrame, previousPolicyTable: Dataset[AudienceModelPolicyRecord], previousPolicyTableDate: LocalDate): DataFrame = {
    // use current date's seed id as active id, should be replaced with other table later
    val activeIds = policyTable.select('SourceId, 'Source, 'CrossDeviceVendorId).distinct().cache
    // get retired sourceId
    val policyTableDayChange = ChronoUnit.DAYS.between(previousPolicyTableDate, date).toInt

    val inActiveIds = previousPolicyTable
      .join(activeIds, Seq("SourceId", "Source", "CrossDeviceVendorId"), "left_anti")
      .withColumn("ExpiredDays", 'ExpiredDays + lit(policyTableDayChange))

    val releasedIds = inActiveIds.filter('ExpiredDays > Config.expiredDays)

    val retentionIds = inActiveIds.filter('ExpiredDays <= Config.expiredDays)
      .withColumn("IsActive", lit(false))
      .withColumn("Tag", lit(Tag.Retention.id))
      .withColumn("SampleWeight", lit(1.0))

    // get new sourceId
    val newIds = activeIds.join(previousPolicyTable.select('SourceId, 'Source, 'CrossDeviceVendorId), Seq("SourceId", "Source", "CrossDeviceVendorId"), "left_anti")
    // get max SyntheticId from previous policy table
    val maxId = previousPolicyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int]
    // generate new synthetic Ids
    val numNewIdNeeded = (newIds.count() - releasedIds.count()).toInt
    // assign available syntheticids randomly to new sourceids
    val allIdsAdded = Random.shuffle(releasedIds.select('SyntheticId).as[Int].collect().toSeq ++ Range.inclusive(maxId + 1, maxId + numNewIdNeeded, 1))
    val getElementAtIndex = udf((index: Long) => allIdsAdded(index.toInt))
    val updatedIds = newIds.withColumn("row_index", (row_number.over(Window.orderBy("SourceId", "CrossDeviceVendorId")) - 1).alias("row_index"))
      .withColumn("SyntheticId", getElementAtIndex($"row_index")).drop("row_index")
      .join(policyTable.drop("SyntheticId"), Seq("SourceId", "Source", "CrossDeviceVendorId"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("IsActive", lit(true))
      .withColumn("Tag", lit(Tag.New.id))
      .withColumn("SampleWeight", lit(1.0)).cache()

    val currentActiveIds = policyTable
      .join(previousPolicyTable.select('SourceId, 'Source, 'CrossDeviceVendorId, 'SyntheticId, 'IsActive), Seq("SourceId", "Source", "CrossDeviceVendorId"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("Tag", when('IsActive, lit(Tag.Existing.id)).otherwise(lit(Tag.Recall.id)))
      .withColumn("IsActive", lit(true))
      .withColumn("SampleWeight", lit(1.0)) // todo optimize this with performance/monitoring

    val updatedPolicyTable = updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)

    updatedPolicyTable
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
}

object RSMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.RSM) {

  val bitwiseOrAgg = udaf(BitwiseOrAgg)

  override def retrieveSourceData(date: LocalDate): DataFrame = {

    val recentVersion =
      if (Config.seedMetaDataRecentVersion != null) Config.seedMetaDataRecentVersion
      else S3Utils.queryCurrentDataVersion(Config.seedMetadataS3Bucket, Config.seedMetadataS3Path)

    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + "/" + Config.seedMetadataS3Path + "/" + recentVersion

    val uniqueTDIDs = getBidImpUniqueTDIDs(date)

    val personGraph = readGraphData(date, CrossDeviceVendor.IAV2Person)(spark)

    val sampledPersonGraph = personGraph
      .select('uiid.alias("TDID"), 'groupId)
      .where(samplingFunction('TDID))
      .withColumnRenamed("groupId", "personId")
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)

    val householdGraph = readGraphData(date, CrossDeviceVendor.IAV2Household)(spark)

    val sampledHouseholdGraph = householdGraph
      .select('uiid.alias("TDID"), 'groupId)
      .where(samplingFunction('TDID))
      .withColumnRenamed("groupId", "householdId")
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)

    val sampledGraph = sampledPersonGraph
      .join(sampledHouseholdGraph, Seq("TDID"), "outer")
      .cache()

    val personGraphMapping = generateGraphMapping(personGraph)
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
      .cache()

    val householdGraphMapping = generateGraphMapping(householdGraph)
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
      .cache()

    val policyMetaTable = spark.read.parquet(seedDataFullPath)
      .select('SeedId, 'TargetingDataId, 'Count, 'Path)
      .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
      .where('Count >= lit(Config.seedProcessLowerThreshold) && 'Count <= lit(Config.seedProcessUpperThreshold))
      .collect()
      .par

    policyMetaTable.tasksupport = new scala.collection.parallel.ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(Config.seedJobParallel))

    val policyTable = policyMetaTable.map(
        record => {
          val seedDataFullPath = record.getAs[String]("Path")
          val seedId = record.getAs[String]("SeedId")

          try {
            val seedData = spark.read.parquet(seedDataFullPath)
              .where(shouldTrackTDID('UserId))
              .select('UserId)
              .withColumnRenamed("UserId", "TDID")
              .repartition(Config.bidImpressionRepartitionNum, 'TDID)
              .distinct()
              .cache()

            var fullSeedData = seedData
              .where(samplingFunction('TDID))
              .withColumn("tag", dataSourceTag(CrossDeviceVendor.None))

            // when the seed count is huge, ignore the seed
            if (record.getAs[Long]("Count") <= Config.seedExtendGraphUpperThreshold) {
              val personGraphSeedData = extendSeedWithGraphs(seedData, personGraphMapping, CrossDeviceVendor.IAV2Person)
              Log(() => s"seed: ${seedId} personGraphSeedData: ${personGraphSeedData.count()}")
              val householdGraphSeedData = extendSeedWithGraphs(seedData, householdGraphMapping, CrossDeviceVendor.IAV2Household)
              Log(() => s"seed: ${seedId} householdGraphSeedData: ${householdGraphSeedData.count()}")

              val allGraphSeedData = personGraphSeedData
                .union(householdGraphSeedData)
                .groupBy('TDID)
                .agg(sum('tag).alias("tag"))


              fullSeedData = fullSeedData
                .union(allGraphSeedData)
                .cache()
            } else {
              fullSeedData = fullSeedData.cache()
            }

            val fullSeedDataWithGraph = fullSeedData
              .join(sampledGraph, Seq("TDID"), "inner")
              .as[ExtendedSeedRecord]

            Log(() => s"seed: ${seedId} fullSeedDataWithGraph: ${fullSeedDataWithGraph.count()}")

            ExtendedSeedWritableDataset(seedId)
              .writePartition(
                fullSeedDataWithGraph,
                date,
                saveMode = SaveMode.Overwrite)

            // count tdids from different source
            val count = fullSeedData
              // .filter(samplingFunction('TDID))
              .join(uniqueTDIDs, Seq("TDID"), "inner")
              .agg(
                sum(dataSourceCheck('tag, CrossDeviceVendor.None)),
                sum(dataSourceCheck('tag, CrossDeviceVendor.IAV2Person)),
                sum(dataSourceCheck('tag, CrossDeviceVendor.IAV2Household))
              ).collect()

            seedData.unpersist()
            fullSeedData.unpersist()

            var data = Seq(
              (
                count(0).getLong(0) * (userDownSampleBasePopulation / userDownSampleHitPopulation),
                CrossDeviceVendor.None.id
              ))

            if (count(0).getLong(1) > 0) {
              data ++= Seq(
                (
                (count(0).getLong(0) + count(0).getLong(1)) * (userDownSampleBasePopulation / userDownSampleHitPopulation),
                CrossDeviceVendor.IAV2Person.id)
              )
            }

            if (count(0).getLong(2) > 0) {
              data ++= Seq(
                (
                  (count(0).getLong(0) + count(0).getLong(2)) * (userDownSampleBasePopulation / userDownSampleHitPopulation),
                  CrossDeviceVendor.IAV2Household.id)
              )
            }

            Some(
              data
                .toDF("ActiveSize", "CrossDeviceVendorId")
                .withColumn("SeedId", lit(record.getAs[String]("SeedId")))
                .withColumn("Size", lit(record.getAs[Long]("Count")))
                .withColumn("TargetingDataId", lit(record.getAs[Long]("TargetingDataId")))
                .coalesce(1)
            )
          } catch {
            case e: Exception => {
              println(s"Caught exception of type ${e.getClass} on seed ${record.getAs[String]("SeedId")}")
              e.printStackTrace()
              throw e
              //              None
            }
          }
        }
      ).toArray.flatten.reduce(_ unionAll _)
      .withColumnRenamed("SeedId", "SourceId")
      .repartition(Config.seedRepartitionNum, 'SourceId)
      .withColumn("Source", lit(DataSource.Seed.id))
      .withColumn("GoalType", lit(GoalType.Relevance.id))
      .cache()

    personGraph.unpersist()
    householdGraph.unpersist()
    sampledGraph.unpersist()
    personGraphMapping.unpersist()
    householdGraphMapping.unpersist()
    householdGraph.unpersist()
    uniqueTDIDs.unpersist()

    policyTable
  }

  private def extendSeedWithGraphs(seedData: DataFrame, graph: DataFrame, crossDeviceVendor: CrossDeviceVendor): DataFrame = {
    val seedWithGraph = graph
      .join(seedData, Seq("TDID"), "inner")
      .select(explode('co_TDIDs).alias("TDID"))
      .join(seedData, Seq("TDID"), "leftanti")
      .withColumn("tag", dataSourceTag(crossDeviceVendor))

    seedWithGraph
  }
}

object AEMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.AEM) {
  val conversionSamplingFunction = shouldConsiderTDID3(config.getInt("userDownSampleHitPopulationAEMConversion", default = 1000000), config.getString("userDownSampleSaltAEMConversion", "KUQ3@F"))(_)

  override def retrieveSourceData(date: LocalDate): DataFrame = {
    retrieveConversionData(date: LocalDate)
  }

  private def retrieveConversionData(date: LocalDate): DataFrame = {
    // conversion
    val conversionSize = ConversionDataset(defaultCloudProvider)
      .readRange(date.minusDays(Config.conversionLookBack).atStartOfDay(), date.plusDays(1).atStartOfDay())
      .select('TDID, 'TrackingTagId)
      .filter(conversionSamplingFunction('TDID))
      .groupBy('TrackingTagId)
      .agg(
        countDistinct('TDID)
          .alias("Size"))

    val policyTable = conversionSize
      .withColumn("TargetingDataId", lit(-1))
      .withColumn("Source", lit(DataSource.Conversion.id))
      .withColumn("GoalType", lit(GoalType.CPA.id))
      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))

    policyTable
  }
}
