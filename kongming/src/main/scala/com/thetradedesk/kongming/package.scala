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

  val KongmingApplicationName = "KoaV4Conversion"
  val RunTimeGaugeName = "run_time_seconds"
  val OutputRowCountGaugeName = "output_rows_written"

  var writeThroughHdfs = config.getBoolean("writeThroughHdfs", true)

  var date = config.getDate("date" , LocalDate.now())
  val RoundUpTimeUnit = "minute"
  val policyDate = config.getDate("policydate" , LocalDate.parse("2022-03-15"))
  val samplingSeed = config.getLong(path = "samplingSeed", 42)
  val IdentityHouseholdUnmatchedToken = "unmatched"
  val JobExperimentName = config.getString("jobExperimentName", "")

  def getJobNameWithExperimentName(jobName: String): String = {
    if (JobExperimentName == "") jobName else s"Experiment=$JobExperimentName-$jobName"
  }

  def getExperimentPath(experimentName: String): String = {
    var result:String = ""

    if (experimentName.trim.nonEmpty)
    {
      result = result.concat(s"experiment=$experimentName/")
    }

    result
  }


  //TODO: may add some indicator on the list of fields to join based on policy.
  def multiLevelJoinWithPolicy[T: Encoder](
                                            inputDataSet: Dataset[_]
                                            , adGroupPolicy: Dataset[_]
                                            , joinType: String
                                          ): Dataset[T] = {
    // TODO: will need to enrich this logic but for now assuming hierarchical structure of keys
    // Caution: there might be cases where adgroupid, campaignId, advertiserId collide. Will need to resolve that at some point.
    // for now only use adgroup and campaign to start with.
    inputDataSet.join(broadcast(adGroupPolicy.filter(col("DataAggKey") === lit("AdGroupId"))), col("DataAggValue") === col("AdGroupId"), joinType).union(
      inputDataSet.join(broadcast(adGroupPolicy.filter(col("DataAggKey") === lit("CampaignId"))), col("DataAggValue") === col("CampaignId"), joinType)
    ).union(
      inputDataSet.join(broadcast(adGroupPolicy.filter(col("DataAggKey") === lit("AdvertiserId"))), col("DataAggValue") === col("AdvertiserId"), joinType)
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
}
