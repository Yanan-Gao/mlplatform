package job.monitoring

import com.thetradedesk.plutus.data.transform.monitoring.BackoffMonitoringTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.opentelemetry.OtelClient

object BackoffMonitoringJob {
  val date = config.getDateRequired("date")
  val otelClient = new OtelClient("BackoffMonitoring", "BackoffMonitoringJob")
  val fileCount = config.getInt("fileCount", 10)
  val jobDurationGauge = otelClient.createGauge("pc_backoff_monitoring_run_time_seconds", "Job execution time in seconds")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    BackoffMonitoringTransform.transform(date = date, fileCount = fileCount)

    jobDurationGaugeTimer.setDuration()
    otelClient.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
