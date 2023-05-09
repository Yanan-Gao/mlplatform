package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.datasets.Model.Model
import com.thetradedesk.audience.datasets.{AudienceModelPolicyReadableDataset, AudienceModelPolicyWritableDataset, AudienceModelPolicyRecord, ConversionDataset, Model}
import com.thetradedesk.audience.{date, shouldConsiderTDID3, s3Client, ttdEnv}
import com.thetradedesk.audience.utils.SeedUtils
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.util.{Try, Failure, Success, Random}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source

object AudiencePolicyTableGeneratorJob {
  object Config {
    val model = Model.withName(config.getString("modelName", default="RSM"))
    val lookBack = config.getInt("lookBack", default = 3)
    val execution_date = config.getDate("execution_date", date)
    val policyS3Bucket = config.getString("policyS3Bucket", "thetradedesk-mlplatform-us-east-1")
    val policyS3Path = config.getString("policyS3Path", s"configdata/${ttdEnv}/audience/policyTable/${model}/v=1")
    
  }
  
  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }

  def runETLPipeline(): Unit = {

    Config.model match {
        case Model.RSM => {        
          val policyTable = RSMPolicyTableGenerator.retrieveSourceData(Config.execution_date)
          // use current date's seed id as active id, should be replaced with other table later 
          val activeIds = policyTable.select('SourceId, 'CrossDeviceVendorId).distinct()
          val policyTableResult = RSMPolicyTableGenerator.addSyntheticId(Config.execution_date, policyTable, activeIds, Config.lookBack, DataSource.Seed.id)
          AudienceModelPolicyReadableDataset(Model.RSM)
          .writePartition(
            policyTableResult.as[AudienceModelPolicyRecord],
            Config.execution_date,
            saveMode = SaveMode.Overwrite
          )
          RSMPolicyTableGenerator.addPolicyCurrentVersion(Config.execution_date, Config.policyS3Bucket, Config.policyS3Path)
        }
        case Model.AEM => {
          val policyTable = AEMPolicyTableGenerator.retrieveSourceData(Config.execution_date)
          // use current date's seed id as active id, should be replaced with other table later 
          val activeIds = policyTable.select('SourceId, 'CrossDeviceVendorId).distinct()
          val policyTableResult = AEMPolicyTableGenerator.addSyntheticId(Config.execution_date, policyTable, activeIds, Config.lookBack, DataSource.Conversion.id)
          AudienceModelPolicyReadableDataset(Model.AEM)
          .writePartition(
            policyTableResult.as[AudienceModelPolicyRecord],
            Config.execution_date,
            saveMode = SaveMode.Overwrite
          )
          AEMPolicyTableGenerator.addPolicyCurrentVersion(Config.execution_date, Config.policyS3Bucket, Config.policyS3Path)
        }
        case _ => throw new Exception(s"unsupported Model[${Config.model}]")
    }
  }
}


abstract class AudiencePolicyTableGenerator(model: Model) {
  
  object Config {
    // detect recent seed metadata path in airflow and pass to spark job
    val seedMetaDataRecentVersion = config.getString("seedMetaDataRecentVersion", null)
    val seedMetadataS3Bucket = config.getString("seedMetadataS3Bucket", "ttd-datprd-us-east-1")
    val seedMetadataS3Path = config.getString("seedMetadataS3Path", "/prod/data/SeedDetail/v=1/")
    // conversion data look back days
    val conversionLookBack = config.getInt("conversionLookBack", 1)
    val expiredDays = config.getInt("expiredDays", default = 7)
  }

  def readPreviousPolicyTable(date: LocalDate, source: Int): Try[org.apache.spark.sql.Dataset[AudienceModelPolicyRecord]] = {
    Try {
      AudienceModelPolicyWritableDataset(model)
    .readPartition(date.minusDays(1))(spark)
    // .where('IsActive)
    // TODO: Later we should one model to handle all KPI tasks (Conversion, SIB, etc), so we should have unique synthetic id within the whole model 
    .where('Source===source)
    }
  }

  def addPolicyCurrentVersion(date: LocalDate, s3Bucket: String, s3Path:String) = {
    
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateStr = date.format(formatter)
    val s3Obj = Try(s3Client.getObject(s3Bucket, s3Path + "/_CURRENT"))

    s3Obj match {
      case Failure(_) => {
        s3Client.putObject(s3Bucket, s3Path + "/_CURRENT", s"date=${dateStr}")
      }
      case Success(value) => {
        val existingVersion = Source.fromInputStream(value.getObjectContent).getLines.mkString.trim
        if(dateStr>existingVersion.split("=")(1)) {
          s3Client.putObject(s3Bucket, s3Path + "/_CURRENT", s"date=${dateStr}")
        }
      }
    }
  }
  
  // todo assign synthetic id, weight, tag, and isActive

  def updateSyntheticId(activeIds: DataFrame, policyTable: DataFrame, previousPolicyTable: org.apache.spark.sql.Dataset[AudienceModelPolicyRecord]): DataFrame = {
    // get retired sourceId 
    val inActiveIds = previousPolicyTable.select('SourceId,'CrossDeviceVendorId, 'SyntheticId, 'ExpiredDays).join(activeIds, Seq("SourceId", "CrossDeviceVendorId"), "left_anti")
    val releasedIds = inActiveIds.filter('ExpiredDays>=Config.expiredDays)
    val retentionIds = inActiveIds.filter('ExpiredDays<Config.expiredDays).withColumn("ExpiredDays", 'ExpiredDays + lit(1))
                                  .withColumn("isActive", lit(false))
                                  .withColumn("Tag", lit(Tag.Existing.id))
                                  .withColumn("SampleWeight", lit(1.0))
    // get new sourceId
    val newIds = activeIds.join(previousPolicyTable.select('SourceId,'CrossDeviceVendorId, 'SyntheticId), Seq("SourceId","CrossDeviceVendorId"), "left_anti")
    // get max SyntheticId from previous policy table
    val maxId = previousPolicyTable.agg(max('SyntheticId)).collect()(0)(0).asInstanceOf[Int]
    // generate new synthetic Ids
    val numNewIdNeeded = (newIds.count() - releasedIds.count()).toInt
    val increasedIds = Range(maxId+1, maxId + numNewIdNeeded+1, 1).toSeq
    // assign avaliable syntheticids randomly to new sourceids
    val allIdsAdded = Random.shuffle(releasedIds.select('SyntheticId).as[Int].collect().toSeq ++ Range(maxId+1, maxId + numNewIdNeeded+1, 1).toSeq)
    val getElementAtIndex = udf((index: Long) => allIdsAdded(index.toInt))
    val updatedIds = newIds.withColumn("row_index", (row_number.over(Window.orderBy("SourceId", "CrossDeviceVendorId"))-1).alias("row_index"))
                            .withColumn("SyntheticId", getElementAtIndex($"row_index")).drop("row_index")
                            .withColumn("ExpiredDays", lit(0))
                            .withColumn("isActive", lit(true))
                            .withColumn("Tag", lit(Tag.New.id))
                            .withColumn("SampleWeight", lit(1.0))
    
    val currentActiveIds = activeIds.join(previousPolicyTable.select('SourceId, 'CrossDeviceVendorId, 'SyntheticId), Seq("SourceId", "CrossDeviceVendorId"), "inner")
                                      .withColumn("ExpiredDays", lit(0))
                                      .withColumn("isActive", lit(true))
                                      .withColumn("Tag", lit(Tag.Existing.id))
                                      .withColumn("SampleWeight", lit(1.0))

    val fullUpdatedIds = updatedIds.unionByName(retentionIds).unionByName(currentActiveIds)

    val updatedPolicyTable = policyTable.join(fullUpdatedIds, Seq("SourceId", "CrossDeviceVendorId"), "inner")
    
    updatedPolicyTable
  }

  def retrieveSourceData(date: LocalDate): DataFrame

  def addSyntheticId(date: LocalDate, policyTable: DataFrame, activeIds: DataFrame, lookBack: Int, source: Int): DataFrame = {
    
    var previousPolicyTable = readPreviousPolicyTable(date, source)

    // if previous x days policy doesn't exit, will generate synethicid from scratch
    // TODO: add more fallback in case previous policy tables missing is due to ETL failure
    previousPolicyTable match {
      case Failure(_) => 
        if(lookBack>0) {
          addSyntheticId(date.minusDays(1), policyTable, activeIds, lookBack-1, source)
        } else {
          val updatedPolicyTable = policyTable.withColumn("SyntheticId", row_number().over(Window.orderBy(rand())))
                                    .withColumn("SampleWeight", lit(1.0))
                                    .withColumn("IsActive", lit(true))
                                    // TODO: update tag info from other signal tables (offline/online monitor, etc)
                                    .withColumn("Tag", lit(Tag.New.id))
                                    .withColumn("ExpiredDays", lit(0))
          updatedPolicyTable
        }
      case Success(data) => 
        val fullUpdatedIds = updateSyntheticId(activeIds, policyTable, data)

        fullUpdatedIds
  }

  }
  


}

object RSMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.RSM) {
  override def retrieveSourceData(date: LocalDate): DataFrame = {

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val dateStr = date.format(formatter)
    
    val seedDataFullPath = "s3a://" + Config.seedMetadataS3Bucket + Config.seedMetadataS3Path + "/" + s"DateTime=${dateStr}*"

    val policyTable = spark.read.parquet(seedDataFullPath)
    // seed data has duplicates, need to deduplicate before seed team fix it
      .withColumn("path_split", split('Path,"="))
      .withColumn("date", substring('path_split(size('path_split)-1), 1, 8))
      .withColumn("TargetingDataId", coalesce('TargetingDataId, lit(-1)))
      .select('SeedId, 'TargetingDataId, 'Count, 'date).distinct()
      .filter('date === dateStr)
      .select('SeedId, 'TargetingDataId, 'Count)
      .where('Count > 0)
      .withColumnRenamed("SeedId", "SourceId")
      .withColumn("Size", 'Count.cast(IntegerType))
      .drop("Count", "path_split", "date")
      .withColumn("Source", lit(DataSource.Seed.id))
      .withColumn("GoalType", lit(GoalType.Relevance.id))
      .withColumn("CrossDeviceVendorId", lit(CrossDeviceVendor.None.id))

    policyTable
  }
}

object AEMPolicyTableGenerator extends AudiencePolicyTableGenerator(Model.AEM) {
  val conversionSamplingFunction = shouldConsiderTDID3(_, config.getInt(s"userDownSampleHitPopulationAEMConversion", default = 1000000))

  override def retrieveSourceData(date: LocalDate): DataFrame = {
    retrieveConversionData(date: LocalDate)
  }

  private def retrieveConversionData(date: LocalDate) : DataFrame = {
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
