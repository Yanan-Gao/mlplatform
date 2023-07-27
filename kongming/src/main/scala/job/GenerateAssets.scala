package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{DailyBidsImpressionsDataset, ImpressionPlacementIdDataset}
import com.thetradedesk.kongming.transform.AssetsTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient


object GenerateAssets {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("GenerateAssetsTable"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    // input params
    val lookbackDays = config.getInt(path="assetsLookback", 15)

    val bidsImpressions = DailyBidsImpressionsDataset().readRange(date.minusDays(lookbackDays), date, isInclusive = true)

    // generate top placement id as model assets
    val impressionPlacementDF = AssetsTransform.topPlacementPerSite(bidsImpressions)(prometheus)
    val impressionPlacementIdRows = ImpressionPlacementIdDataset().writePartition(impressionPlacementDF, date, Some(1))

    outputRowsWrittenGauge.labels("ImpressionPlacementIdDataset").set(impressionPlacementIdRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }

}
