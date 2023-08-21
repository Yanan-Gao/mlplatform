package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import java.time.LocalDate

object DailyResearchAdGroupPolicy extends KongmingBaseJob {

  override def jobName: String = "DailyResearchAdGroupPolicy"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    // default dataset for adgroup policy table which will be updated on a monthly basis
    val defaultPolicyDate = LocalDate.of(2022, 3,15)
    val historyAdGroupPolicy = AdGroupPolicyDataset(JobExperimentName).readDate(defaultPolicyDate).select($"ConfigKey", $"ConfigValue")

    val latestAdGroupPolicy = AdGroupPolicyDataset().readLatestPartitionUpTo(date)

    val adGroupPolicy = historyAdGroupPolicy.join(latestAdGroupPolicy, Seq("ConfigKey", "ConfigValue"), "inner").selectAs[AdGroupPolicyRecord]

    val adGroupPolicyRows = AdGroupPolicyDataset(JobExperimentName).writePartition(adGroupPolicy, date, Some(1))

    Array(adGroupPolicyRows)

  }

  def getSettings(adGroupPolicy: Dataset[AdGroupPolicyRecord],
                  adGroupDS: Dataset[AdGroupRecord],
                  campaignDS: Dataset[CampaignRecord],
                  advertiserDS: Dataset[AdvertiserRecord]
                 ): Dataset[AdGroupPolicyRecord] = {
    val advertiserAttributionWindow =
      advertiserDS
        .select($"AdvertiserId", $"AttributionClickLookbackWindowInSeconds", $"AttributionImpressionLookbackWindowInSeconds")

    adGroupPolicy.drop("AttributionClickLookbackWindowInSeconds", "AttributionImpressionLookbackWindowInSeconds")
      .join(adGroupDS, adGroupPolicy("ConfigValue") === adGroupDS("AdGroupId"), "left")
      .join(campaignDS, Seq("CampaignId"), "left")
      .join(advertiserAttributionWindow,
        Seq("AdvertiserId"),
        "left"
      )
      .selectAs[AdGroupPolicyRecord]
  }
}
