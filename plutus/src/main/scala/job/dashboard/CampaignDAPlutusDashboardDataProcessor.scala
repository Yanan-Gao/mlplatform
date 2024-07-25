package job.dashboard

import com.thetradedesk.plutus.data.transform.dashboard.CampaignDAPlutusDashboardDataTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object CampaignDAPlutusDashboardDataProcessor {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 500)

  val prometheus = new PrometheusClient("PlutusDashboard", "CampaignDAPlutusDashboardDataProcessor")
  val jobDurationGauge = prometheus.createGauge("plutus_dashboard_run_time_seconds", "Job execution time in seconds", labelNames="jobname")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.labels("CampaignDAPlutusDashboardDataProcessor").startTimer()

    CampaignDAPlutusDashboardDataTransform.transform(date, fileCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    spark.stop()
  }
}
