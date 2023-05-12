package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import java.time.LocalDate

object DailyResearchAdGroupPolicy {

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("DailyResearchAdGroupPolicy"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    // default dataset for adgroup policy table which will be updated on a monthly basis
    val defaultPolicyDate = LocalDate.of(2022, 3,15)
    val historyAdGroupPolicy = AdGroupPolicyDataset().readDate(defaultPolicyDate)
    //load advertiser dataset for attribution window and campaign data for aux
    val advertiserDS = AdvertiserDataSet().readLatestPartitionUpTo(date)
    // read campaign table to get setting, need to remove duplicated rows vs advertiser settings
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date).selectAs[CampaignRecord]
    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date)

    val adGroupPolicy = getSettings(historyAdGroupPolicy, adGroupDS, campaignDS, advertiserDS)

    val adGroupPolicyRows = AdGroupPolicyDataset().writePartition(adGroupPolicy, date, Some(1))

    outputRowsWrittenGauge.labels("DailyResearchAdGroupPolicy").set(adGroupPolicyRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
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
