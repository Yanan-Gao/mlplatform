package job.dashboard

import com.thetradedesk.plutus.data.transform.dashboard.DAPlutusDashboardDataTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import TTDSparkContext.spark

object DAPlutusDashboardDataProcessor {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 500)

  val prometheus = new PrometheusClient("PlutusDashboard", "DAPlutusDashboardDataProcessor")
  val jobDurationGauge = prometheus.createGauge("plutus_dashboard_run_time_seconds", "Job execution time in seconds", labelNames="jobname")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    DAPlutusDashboardDataTransform.transform(date, fileCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    spark.stop()
  }
}
