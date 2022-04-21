package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk._
import org.apache.spark.sql.{Dataset, SparkSession}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.DataFrame

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

  def readHardCodedDataset(date: LocalDate)(implicit spark: SparkSession): Dataset[AdGroupPolicyRecord] = {

    val hardCodedPolicy = spark.read
      .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
      .csv(s"${S3Path}/date=${date.toString}")

    //load advertiser dataset for attribution window and campaign data for aux
    val advertiserDS = loadParquetData[AdvertiserRecord](AdvertiserDataset.S3Path, kongming.date)
    // read campaign table to get setting
    val campaignDS = loadParquetData[CampaignRecord](CampaignDataset.S3Path, kongming.date)
    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = loadParquetData[AdGroupRecord](AdGroupDataset.ADGROUPS3, kongming.date)

    val adGroupPolicy = getSettings(hardCodedPolicy, adGroupDS, campaignDS, advertiserDS)

    adGroupPolicy
  }
}
