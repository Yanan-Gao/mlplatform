package job

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.MetadataDataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.prometheus.PrometheusClient

abstract class KongmingBaseJob {

  def jobName: String = ""

  def getPrometheus: PrometheusClient = {
    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName(jobName))
    prometheus
  }

  def runTransform(args: Array[String]): Array[(String, Long)] = Array(("", 0))

  def main(args: Array[String]): Unit = {

    val prometheus = getPrometheus
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName,
      "Number of rows written", "DataSet")

    val rows = runTransform(args)

    rows.foreach { case (datasetName, rowCount) =>
      outputRowsWrittenGauge.labels(datasetName).set(rowCount)
    }

    val runTime = jobDurationGaugeTimer.setDuration()
    MetadataDataset().writeRecord(runTime.toLong, date, MetadataType.runTime, jobName)
    prometheus.pushMetrics()

    spark.stop()
  }
}
