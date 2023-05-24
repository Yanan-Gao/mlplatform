package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.utils.S3Utils
import com.thetradedesk.audience.{dateTime, audienceVersionDateFormat, shouldConsiderTDID3, ttdEnv}
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

  object Config {
    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
    val seedMetadataS3Bucket = S3Utils.refinePath(config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1"))
    val seedMetadataS3Path = S3Utils.refinePath(config.getString("seedMetadataS3Path", "prod/data/SeedDetail/v=1/"))
    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 1)
    val expiredDays = config.getInt("expiredDays", default = 7)
    val policyTableLookBack = config.getInt("policyTableLookBack", default = 3)
    val policyS3Bucket = S3Utils.refinePath(config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1"))
    val policyS3Path =S3Utils.refinePath(config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1"))
    val maxVersionsToKeep = config.getInt("maxVersionsToKeep", 30)
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
    val activeIds = policyTable.select('SourceId, 'Source, 'CrossDeviceVendorId).distinct()
    // get retired sourceId
    val policyTableDayChange = ChronoUnit.DAYS.between(date, previousPolicyTableDate).toInt

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
      .withColumn("SampleWeight", lit(1.0))

    val currentActiveIds = previousPolicyTable.join(activeIds, Seq("SourceId", "Source", "CrossDeviceVendorId"), "inner")
      .withColumn("ExpiredDays", lit(0))
      .withColumn("Tag", when('IsActive, lit(Tag.Existing.id)).otherwise(lit(Tag.Recall.id)))
      .withColumn("IsActive", lit(true))
      .withColumn("SampleWeight", lit(1.0)) // todo optimize this with performance/monitoring

    val updatedPolicyTable = updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)

    updatedPolicyTable
  }

  def retrieveSourceData(date: LocalDate): DataFrame

  private def allocateSyntheticId(dateTime: LocalDateTime, policyTable: DataFrame): DataFrame = {
    val recentVersionOption = availablePolicyTableVersions.find(_.isBefore(dateTime))
    if (recentVersionOption.isDefined) {
      val previousPolicyTable = AudienceModelPolicyReadableDataset(model)
        .readSinglePartition(recentVersionOption.get)(spark)
      updateSyntheticId(dateTime.toLocalDate, policyTable, previousPolicyTable, recentVersionOption.get.toLocalDate)
    } else {
      val updatedPolicyTable = policyTable
        .withColumn("SyntheticId", row_number().over(Window.orderBy(rand())))
        .withColumn("SampleWeight", lit(1.0))
        .withColumn("IsActive", lit(true))
        // TODO: update tag info from other signal tables (offline/online monitor, etc)
        .withColumn("Tag", lit(Tag.New.id))
        .withColumn("ExpiredDays", lit(0))
      updatedPolicyTable
    }
  }
}

object RSMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.RSM) {
  override def retrieveSourceData(date: LocalDate): DataFrame = {

    val recentVersion =
      if (Config.seedMetaDataRecentVersion != null) Config.seedMetaDataRecentVersion
      else S3Utils.queryCurrentDataVersion(Config.seedMetadataS3Bucket, Config.seedMetadataS3Path)

    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + "/" + Config.seedMetadataS3Path + "/" + recentVersion

    val policyTable = spark.read.parquet(seedDataFullPath)
      .select('SeedId, 'TargetingDataId, 'Count)
      .where('Count > 0)
      .groupBy('SeedId, 'TargetingDataId)
      .agg(max('Count).alias("Count"))
      .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
      .withColumnRenamed("SeedId", "SourceId")
      .withColumn("Size", 'Count.cast(IntegerType))
      .drop("Count", "SeedId")
      .withColumn("Source", lit(DataSource.Seed.id))
      .withColumn("GoalType", lit(GoalType.Relevance.id))
      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))
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
