package com.thetradedesk.kongming.datasets

import com.thetradedesk._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate

final case class AdGroupPolicyRecord(ConfigKey: String,
                                     ConfigValue: String,
                                     DataAggKey: String,
                                     DataAggValue: String,
                                     CrossDeviceUsage: Boolean,
                                     MinDailyConvCount: Int,
                                     MaxDailyConvCount: Int,
                                     LastTouchCount: Int,
                                     DataLookBack: Int,
                                     PositiveSamplingRate: Double,
                                     NegativeSamplingMethod: String,
                                     CrossDeviceConfidenceLevel: Double,
                                     FetchOlderTrainingData: Boolean,
                                     OldTrainingDataEpoch: Int,
                                     AttributionClickLookbackWindowInSeconds: Int,
                                     AttributionImpressionLookbackWindowInSeconds: Int
                                    )

object AdGroupPolicyDataset {
  val S3Path: String = "s3a://thetradedesk-useast-hadoop/cxw/foa/datasets/adgroupPolicies"

  //TODO: may need to add these changes into policy generation eventually.
  def getSettings(adGroupPolicy: DataFrame,
                  adGroupDS: Dataset[AdGroupRecord],
                  campaignDS: Dataset[CampaignRecord],
                  advertiserDS: Dataset[AdvertiserRecord]
                 ):Dataset[AdGroupPolicyRecord]={
    val advertiserAttributionWindow =
      advertiserDS
        .select($"AdvertiserId", $"AttributionClickLookbackWindowInSeconds", $"AttributionImpressionLookbackWindowInSeconds")

    adGroupPolicy
      .join(adGroupDS, adGroupPolicy("ConfigValue")===adGroupDS("AdGroupId"),"left")
      .join(campaignDS, Seq("CampaignId"), "left")
      .join( advertiserAttributionWindow,
        Seq("AdvertiserId"),
        "left"
      )
      .selectAs[AdGroupPolicyRecord]

  }

  def readHardCodedDataset(date: LocalDate): Dataset[AdGroupPolicyRecord] = {

    val hardCodedPolicy = spark.read
      .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(s"${S3Path}/date=${date.toString}")

    //load advertiser dataset for attribution window and campaign data for aux
    val advertiserDS = AdvertiserDataSet().readLatestPartition()
    // read campaign table to get setting, need to remove duplicated rows vs advertiser settings
    val campaignDS = CampaignDataSet().readLatestPartition().selectAs[CampaignRecord]
    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()

    val adGroupPolicy = getSettings(hardCodedPolicy, adGroupDS, campaignDS, advertiserDS)

    adGroupPolicy
  }
}
