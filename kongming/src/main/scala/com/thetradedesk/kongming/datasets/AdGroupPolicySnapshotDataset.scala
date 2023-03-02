package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate

final case class AdGroupPolicySnapshotRecord(ConfigKey: String,
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
                                            )

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

case class AdGroupPolicySnapshotDataset(experimentName: String = "") extends KongMingDataset[AdGroupPolicySnapshotRecord](
  s3DatasetPath = "dailyadgrouppolicy/v=1",
  experimentName = config.getString("ttd.AdGroupPolicySnapshotDataset.experimentName", "")
) {
  //TODO: may need to add these changes into policy generation eventually.
  def getSettings(adGroupPolicy: Dataset[AdGroupPolicySnapshotRecord],
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

  def readDataset(date: LocalDate): Dataset[AdGroupPolicyRecord] = {

    val hardCodedPolicy = readDate(date)

    //load advertiser dataset for attribution window and campaign data for aux
    val advertiserDS = AdvertiserDataSet().readLatestPartitionUpTo(date)
    // read campaign table to get setting, need to remove duplicated rows vs advertiser settings
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date).selectAs[CampaignRecord]
    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date)

    getSettings(hardCodedPolicy, adGroupDS, campaignDS, advertiserDS)
  }
}
