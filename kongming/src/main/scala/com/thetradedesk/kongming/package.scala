package com.thetradedesk

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.kongming.datasets.{AdGroupPolicyRecord, AdGroupRecord}
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

  // TODO: config to KoaV4ROAS when task="roas"
  val KongmingApplicationName = "KoaV4Conversion"
  val RunTimeGaugeName = "run_time_seconds"
  val OutputRowCountGaugeName = "output_rows_written"
  val LogsDiscrepancyCountGaugeName = "logs_discrepancy_count"

  var writeThroughHdfs = config.getBoolean("writeThroughHdfs", true)

  // TODO: set roas for ROAS
  // task: cpa (default), roas
  val task = config.getString("task", "cpa")
  var date = config.getDate("date" , LocalDate.now())
  val RoundUpTimeUnit = "minute"
  val policyDate = config.getDate("policydate" , LocalDate.parse("2022-03-15"))
  val samplingSeed = config.getLong(path = "samplingSeed", 42)
  val IdentityHouseholdUnmatchedToken = "unmatched"
  val JobExperimentName = config.getStringOption("jobExperimentName")
  val ExperimentName = config.getStringOption("ttd.experiment")

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

  def preFilteringWithPolicy[T: Encoder](
                                          inputDataSet: Dataset[T]
                                          , adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                          , adGroupDS: Dataset[AdGroupRecord]
                                        ): Dataset[T] ={
    //setup prefiltering of data, based on campaignId for now.
    //TODO: this might subject to change if we wish to have higher level filtering.
    val filterDF = adGroupPolicy.join(adGroupDS, adGroupPolicy("ConfigValue")===adGroupDS("AdGroupId"),"left").select("AdvertiserId").distinct

    val prefilteredDS = inputDataSet
      .join(broadcast(filterDF), Seq("AdvertiserId"), "inner")
      .selectAs[T]

    prefilteredDS
  }

  class PartitionCount {
    var AdGroupPolicy = 1
    var AdGroupPolicyMapping = 100
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
      partCount.trainSet = 500
      partCount.valSet = 100
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

}

object MetadataType extends Enumeration {
  type MetadataType = Value

  val rowCount = Value("RowCount")
  val runTime = Value("RunTime")

}
