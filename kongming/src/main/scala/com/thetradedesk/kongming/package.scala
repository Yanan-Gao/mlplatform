package com.thetradedesk


import com.thetradedesk.kongming.datasets.AdGroupPolicyRecord
import com.thetradedesk.kongming.datasets.AdGroupRecord
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Encoder
import com.thetradedesk.spark.sql.SQLFunctions._

import java.time.LocalDate


package object kongming {
  var date = config.getDate("date" , LocalDate.now())
  var ttdEnv = config.getString("ttd.env" , "dev")
  val RoundUpTimeUnit = "minute"
  val policyDate = config.getDate("policydate" , LocalDate.parse("2022-06-08"))

  //TODO: may add some indicator on the list of fields to join based on policy.
  def multiLevelJoinWithPolicy[T: Encoder](
                                            inputDataSet: Dataset[_]
                                            , adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                          ): Dataset[T] = {
    // TODO: will need to enrich this logic but for now assuming hierarchical structure of keys
    // Caution: there might be cases where adgroupid, campaignId, advertiserId collide. Will need to resolve that at some point.
    // for now only use adgroup and campaign to start with.
    val fieldsToJoin = List(("AdGroupId","DataAggValue"), ("CampaignId","DataAggValue"))//, ("AdvertiserId","DataAggValue"))
    val joinCondition = fieldsToJoin.map(x => col(x._1) === col(x._2)).reduce(_ || _)

    inputDataSet
      .join(broadcast(adGroupPolicy), joinCondition, "inner")
      .selectAs[T]
  }


  def multiLevelJoinWithPolicy[T: Encoder](
                                            inputDataSet: Dataset[_]
                                            , adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                            , filterCondition: Column
                                          ): Dataset[T] = {
    // TODO: will need to enrich this logic but for now assuming hierarchical structure of keys
    // Caution: there might be cases where adgroupid, campaignId, advertiserId collide. Will need to resolve that at some point.
    // for now only use adgroup and campaign to start with.
    val fieldsToJoin = List(("AdGroupId","DataAggValue"), ("CampaignId","DataAggValue"))//, ("AdvertiserId","DataAggValue"))
    val joinCondition = fieldsToJoin.map(x => col(x._1) === col(x._2)).reduce(_ || _)

    inputDataSet
      .join(broadcast(adGroupPolicy), joinCondition, "inner")
      .filter(filterCondition)
      .selectAs[T]
  }

  def preFilteringWithPolicy[T: Encoder](
                                          inputDataSet: Dataset[T]
                                          , adGroupPolicy: Dataset[AdGroupPolicyRecord]
                                          , adGroupDS: Dataset[AdGroupRecord]
                                        ): Dataset[T] ={
    //setup prefiltering of data, based on campaignId for now.
    //TODO: this might subject to change if we wish to have higher level filtering.
    val filterDF = adGroupPolicy.join(adGroupDS, adGroupPolicy("ConfigValue")===adGroupDS("AdGroupId"),"left").select("CampaignId").distinct

    val prefilteredDS = inputDataSet
      .join(broadcast(filterDF), Seq("CampaignId"), "inner")
      .selectAs[T]

    prefilteredDS
  }
}