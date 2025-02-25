package com.thetradedesk

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingRecord, AdGroupPolicyRecord, AdGroupRecord}
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{SchemaPolicyType, DefaultUseFirstFileSchema, MergeAllFilesSchema, StrictCaseClassSchema}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.{broadcast, col, lit, when}

import java.time.LocalDate

package object kongming {
  val MLPlatformS3Root: String = "s3://thetradedesk-mlplatform-us-east-1/data"
  val BidsImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"

  // TODO: set roas for ROAS
  // task: cpa (default), roas
  val task = config.getString("task", "cpa")
  val KongmingApplicationName = task match {
    case "roas" => "ValueAlgoROAS"
    case _ => "ValueAlgoCPA"
  }
  val BaseFolderPath = task match {
    case "roas" => "roas"
    case _ => "kongming"
  }

  val schemaPolicyStr = config.getString("schemaPolicy", "DefaultUseFirstFileSchema")
  val schemaPolicy: SchemaPolicyType = schemaPolicyStr match {
    case "StrictCaseClassSchema" => StrictCaseClassSchema
    case "MergeAllFilesSchema" => MergeAllFilesSchema
    case _ => DefaultUseFirstFileSchema
  }

  val RunTimeGaugeName = "run_time_seconds"
  val OutputRowCountGaugeName = "output_rows_written"
  val LogsDiscrepancyCountGaugeName = "logs_discrepancy_count"

  var writeThroughHdfs = config.getBoolean("writeThroughHdfs", true)

  var date = config.getDate("date" , LocalDate.now())
  val RoundUpTimeUnit = "minute"
  val policyDate = config.getDate("policydate" , LocalDate.parse("2022-03-15"))
  val samplingSeed = config.getLong(path = "samplingSeed", 42)
  val IdentityHouseholdUnmatchedToken = "unmatched"
  val JobExperimentName = config.getStringOption("jobExperimentName")
  val ExperimentName = config.getStringOption("ttd.experiment")

  val trainingBatchSize = config.getInt("trainingBatchSize", 32768)
  val evalBatchSize = config.getInt("evalBatchSize", 65536)

  object Config {
    val ImpressionLookBack = config.getInt("OutOfSampleAttributeSetGenerator.ImpressionLookBack", 3)
    val AttributionLookBack = config.getInt("OutOfSampleAttributeSetGenerator.AttributionLookBack", 7)
    val OosNegPosRatio = config.getInt("OutOfSampleAttributeSetGenerator.OosNegPosRatio", 1000)
    val CalibrationImpLookBack = config.getInt("GenerateCalibrationData.CalibrationImpLookBack", 12)
    val CalibrationAttLookBack = config.getInt("GenerateCalibrationData.CalibrationAttLookBack", 2)
    val IsotonicPosCntMin = config.getInt("GenerateCalibrationData.IsotonicPosCntMin", 10)
    val IsotonicNegCntMax = config.getInt("GenerateCalibrationData.IsotonicNegCntMax", 100000)
    val IsotonicNegSampleRateMax = config.getDouble("GenerateCalibrationData.IsotonicNegSampleRateMax", 1.0/3)
  }

  def getJobNameWithExperimentName(jobName: String): String = {
    if (JobExperimentName.isEmpty) jobName else s"Experiment=$JobExperimentName-$jobName"
  }

  def getExperimentVersion: Option[String] = {
    if (JobExperimentName.isEmpty) ExperimentName else JobExperimentName
  }

  //TODO: may add some indicator on the list of fields to join based on policy.
  def multiLevelJoinWithPolicy[T: Encoder](
                                            inputDataSet: Dataset[_],
                                            adGroupPolicy: Dataset[_],
                                            joinType: String,
                                            joinKeyName: String = "DataAggKey",
                                            joinValueName: String = "DataAggValue"
                                          ): Dataset[T] = {
    inputDataSet.join(broadcast(adGroupPolicy.filter(col(joinKeyName) === lit("AdGroupId"))), col(joinValueName) === col("AdGroupId"), joinType).union(
      inputDataSet.join(broadcast(adGroupPolicy.filter(col(joinKeyName) === lit("CampaignId"))), col(joinValueName) === col("CampaignId"), joinType)
    ).union(
      inputDataSet.join(broadcast(adGroupPolicy.filter(col(joinKeyName) === lit("AdvertiserId"))), col(joinValueName) === col("AdvertiserId"), joinType)
    ).selectAs[T]
  }

  def multiLevelJoinWithPolicy[T: Encoder](
                                            inputDataSet: Dataset[_]
                                            , adGroupPolicy: Dataset[_]
                                            , filterCondition: Column
                                            , joinType: String
                                          ): Dataset[T] = {
    multiLevelJoinWithPolicy[T](inputDataSet, adGroupPolicy, joinType)
      .filter(filterCondition)
  }

  // Get a minimal subset of the policy table with no change in DataAggValue coverage.
  // Advertisers entirely aggregated on campaign ID is unchanged, but only one of the
  // advertiser rows are included if any campaign is aggregated on advertiser ID.
  def getMinimalPolicy(policy: Dataset[AdGroupPolicyRecord], mapping: Dataset[AdGroupPolicyMappingRecord]): Dataset[AdGroupPolicyRecord] = {
    val policyWithAdvertisers = policy.join(mapping.select("AdGroupId", "CampaignId", "AdvertiserId"), col("ConfigValue") === col("AdGroupId"), "inner")
    val advertiserRows = policyWithAdvertisers.filter(col("DataAggKey") === lit("AdvertiserId"))
      .dropDuplicates("DataAggKey", "DataAggValue")
      .select('ConfigValue.as("AdvertiserConfigValue"), 'AdvertiserId)

    policyWithAdvertisers.join(advertiserRows, Seq("AdvertiserId"), "left")
      .filter(col("AdvertiserConfigValue").isNull || col("ConfigValue") === col("AdvertiserConfigValue"))
      .selectAs[AdGroupPolicyRecord]
  }

  class PartitionCount {
    var AdGroupPolicy = 1
    var AdGroupPolicyMapping = 1
    var DailyConversion = 2000
    var DailyBidRequest = 5000
    var DailyBidsImpressions = 10000
    var DailyNegativeSampledBidRequest = 1000
    var DailyOfflineScoring = 1000
    var trainSet = 1000
    var valSet = 1000
    var TrainSetFeatureMapping = 100
    var DailyPositiveBidRequest = 100
    var DailyPositiveCountSummary = 1
    var OOSTracked = 1000
    var OOSUntracked = 200
    var TrainsetBalanced = 50
    var DailyTrainsetWithFeature = 50
    var ImpressionForIsoReg = 1000
    var SampledImpressionForIsoReg = 2000
    var CvrRescaling = 1
  }

  val partCount = task match {
    case "cpa" => new PartitionCount
    case "roas" => {
      val partCount = new PartitionCount
      partCount.AdGroupPolicyMapping = 1
      partCount.DailyBidRequest = 200
      partCount.DailyBidsImpressions = 1000
      partCount.DailyConversion = 100
      partCount.DailyNegativeSampledBidRequest = 100
      partCount.DailyPositiveBidRequest = 10
      partCount.trainSet = 100
      partCount.valSet = 20
      partCount
    }
    case _ => new PartitionCount
  }

  val ROIGoalTypeId = Map(
    "cpa" -> 5,
    "roas" -> 6
  )

  val CustomGoalTypeId = Map(
    "cpa" -> "CustomCPATypeId",
    "roas" -> "CustomROASTypeId"
  )

  val IncludeInCustomGoal = Map(
    "cpa" -> "IncludeInCustomCPA",
    "roas" -> "IncludeInCustomROAS"
  )

  val optionalFeature = Map(
    0 -> "UserData"
  )

}

object MetadataType extends Enumeration {
  type MetadataType = Value

  val rowCount = Value("RowCount")
  val runTime = Value("RunTime")

}
