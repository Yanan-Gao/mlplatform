package job

import com.thetradedesk.kongming.datasets._
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.{config, defaultCloudProvider}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.transform.ConversionDataDailyTransform
import com.thetradedesk.spark.datasets.sources.datalake.ConversionTrackerVerticaLoadDataSetV4
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark

import java.time.LocalDate


object ConversionDataDailyProcessor extends Logger{
  val graphThreshold = config.getDouble("graphThreshold", default = 0.01)//TODO: verify what's a good value here.

  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("DailyConversion"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    // read conversion data daily
    val conversionDS = ConversionTrackerVerticaLoadDataSetV4(defaultCloudProvider).readDate(date)

    // read campaign conversion reporting column setting
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)

    // read master policy
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)

    // read adgroup table to get adgroup campaign mapping
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, true)
    // read campaign table to get setting
    val campaignDS = CampaignDataSet().readLatestPartitionUpTo(date, true)

    //filter down conversion data and add weights from conversion reporting column table
    //transformedConvDS is returned as cached datasets.
    val (transformedConvDS, idDS) = ConversionDataDailyTransform.dailyTransform(
      conversionDS,
      ccrc,
      adGroupPolicy,
      adGroupDS,
      campaignDS
    )(prometheus)

    //load cross device graph
    val xdDS = CrossDeviceGraphDataset.loadGraph(date, graphThreshold)//TODO: this threshold will need to be based on policy table minimum.
    val xdSubsetDS = CrossDeviceGraphDataset.shrinkGraph(xdDS, idDS) // returned result is cached

    //add xd according to adgroupPolicy
    val transformedConvXD = transformedConvDS.filter($"CrossDeviceAttributionModelId".isNotNull)
    val conversionNonXD = transformedConvDS
      //removing this following filter to include everything without cross device
      //.filter(!$"CrossDeviceUsage")
      .select(
        $"TrackingTagId",
        $"TDID".as("UIID"),
        $"DataAggKey",
        $"DataAggValue",
        $"ConversionTime"
      )
      .as[DailyConversionDataRecord]
      .distinct

    val conversionXD = ConversionDataDailyTransform.addCrossDeviceTransform(
      transformedConvXD
      , xdSubsetDS
    )(prometheus)

    //add distinct to remove rare cases when there's two ids under the same person converted under the same trackingtag.
    val resultDS = conversionNonXD.union(conversionXD).distinct()

    val dailyConversionRows = DailyConversionDataset().writePartition(resultDS, date, Some(2000))

    outputRowsWrittenGauge.labels("DailyConversionDataset").set(dailyConversionRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}