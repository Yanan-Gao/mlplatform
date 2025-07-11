package job.dashboard

import com.thetradedesk.plutus.data.transform.dashboard.PlutusDashboardDataTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import TTDSparkContext.spark

object PlutusDashboardDataProcessor {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 500)

  val prometheus = new PrometheusClient("PlutusDashboard", "PlutusDashboardDataProcessor")
  val jobDurationGauge = prometheus.createGauge("plutus_dashboard_run_time_seconds", "Job execution time in seconds", labelNames="jobname")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.labels("PlutusDashboardDataProcessor").startTimer()

    PlutusDashboardDataTransform.transform(date, fileCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    spark.stop()
  }
}