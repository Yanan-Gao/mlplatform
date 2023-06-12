package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{dateTime, audienceVersionDateFormat, shouldConsiderTDID3, ttdEnv}
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDate, LocalDateTime}
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

  val samplingFunction = shouldConsiderTDID3(config.getInt(s"userDownSampleHitPopulation${model}", default = 1000000), config.getStringRequired(s"saltToSampleUser${model}"))(_)
  
  object Config {
    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
    val seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
    val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
    val seedRawDataS3Bucket = S3Utils.refinePath(config.getString("seedRawDataS3Bucket", "ttd-datprd-us-east-1"))
    val seedRawDataS3Path = S3Utils.refinePath(config.getString("seedRawDataS3Path", "prod/data/Seed/v=1/SeedId="))
    val seedRawDataRecentVersion = config.getString("seedRawDataRecentVersion", null)
    val policyTableResetSyntheticId= config.getBoolean("policyTableResetSyntheticId", false)
    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 1)
    val expiredDays = config.getInt("expiredDays", default = 7)
    val policyTableLookBack = config.getInt("policyTableLookBack", default = 3)
    val policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val policyS3Path =S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1"))
    val maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30)
    val bidImpressionRepartitionNum = config.getInt("bidImpressionRepartitionNum", 8192)
    val seedRepartitionNum = config.getInt("seedRepartitionNum", 500)
    

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
    updatePolicyCurrentVersion(dateTime)
  }

  def getBidImpUniqueTDIDs(date: LocalDate) = {
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

    val uniqueTDIDs = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date.minusDays(1), source = Some(GERONIMO_DATA_SOURCE))
      .withColumnRenamed("UIID", "TDID")
      // .filter(samplingFunction('TDID))
      .select('TDID)
      .repartition(Config.bidImpressionRepartitionNum, 'TDID)
      .distinct()
      .cache()

    uniqueTDIDs
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
    val recentVersionOption = if(Config.seedMetaDataRecentVersion != null) Some(LocalDateTime.parse(Config.seedMetaDataRecentVersion, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSS") ).toLocalDate.atStartOfDay()) 
                              else availablePolicyTableVersions.find(_.isBefore(dateTime))

    if (!(recentVersionOption.isDefined) || Config.policyTableResetSyntheticId)  {
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
  override def retrieveSourceData(date: LocalDate): DataFrame = {

    val recentVersion =
      if (Config.seedMetaDataRecentVersion != null) Config.seedMetaDataRecentVersion
      else S3Utils.queryCurrentDataVersion(Config.seedMetadataS3Bucket, Config.seedMetadataS3Path)

    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + "/" + Config.seedMetadataS3Path + "/" + recentVersion

    val uniqueTDIDs = getBidImpUniqueTDIDs(date)

    val policyMetaTable = spark.read.parquet(seedDataFullPath)
      .select('SeedId, 'TargetingDataId, 'Count)
      .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
      .where('Count > 0)
      .groupBy('SeedId, 'TargetingDataId)
      .agg(max('Count).alias("Count"))
      .select('SeedId, 'TargetingDataId, 'Count)
      .collect()

    val policyTable = policyMetaTable.par.map (
      record => { 
        val seedDataPath = Config.seedRawDataS3Path + record.getAs[String]("SeedId")
        val seedDataFullPath = "s3a://" + Config.seedRawDataS3Bucket + "/" + seedDataPath
        val recentVersion = if (Config.seedRawDataRecentVersion != null) Config.seedRawDataRecentVersion
          else S3Utils.queryCurrentDataVersion(Config.seedRawDataS3Bucket, seedDataPath)
        
        try {
          Some(spark.read.parquet(seedDataFullPath + "/" + recentVersion)
          .select('UserId)
          .withColumnRenamed("UserId", "TDID")
          // .filter(samplingFunction('TDID))
          .join(uniqueTDIDs, Seq("TDID"), "inner")
          .withColumn("SeedId", lit(record.getAs[String]("SeedId")))
          .groupBy('SeedId)
          .count()
          .withColumnRenamed("count", "ActiveSize")
          .withColumn("Size", lit(record.getAs[Long]("Count")))
          .withColumn("TargetingDataId", lit(record.getAs[Long]("TargetingDataId")))
          .select('SeedId, 'TargetingDataId, 'Size, 'ActiveSize))
        } catch {
          case e: Exception => {
            println(s"Caught exception of type ${e.getClass} on seed ${record.getAs[String]("SeedId")}")
            None
          }
          
        }
      }
    ).toArray.flatten.reduce(_ unionAll _)
          .withColumnRenamed("SeedId", "SourceId")
          .repartition(Config.seedRepartitionNum, 'SourceId)
          .withColumn("ActiveSize", 'ActiveSize.cast(LongType))
          .withColumn("Source", lit(DataSource.Seed.id))
          .withColumn("GoalType", lit(GoalType.Relevance.id))
          .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
          .cache()

    policyTable
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
