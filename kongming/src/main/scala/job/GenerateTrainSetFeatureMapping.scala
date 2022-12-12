package job

import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.TrainSetFeatureMappingDataset
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.TrainSetFeatureMappingTransform
import com.thetradedesk.spark.TTDSparkContext.spark

import java.time.LocalDate

object GenerateTrainSetFeatureMapping {
  val fixedDateParquet = LocalDate.of(2022, 11,15)
  def main(args: Array[String]): Unit = {
    val prometheus = new PrometheusClient(KongmingApplicationName, "GenerateTrainSetFeatureMapping")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidsImpressions = DailyBidsImpressionsDataset().readDate(date)
    val featureMappings = TrainSetFeatureMappingTransform.dailyTransform(bidsImpressions)(prometheus)
    val featureMappingRows = TrainSetFeatureMappingDataset().writePartition(featureMappings, fixedDateParquet, Some(100))
    outputRowsWrittenGauge.labels("TrainSetFeatureMappingDataset").set(featureMappingRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }


}
