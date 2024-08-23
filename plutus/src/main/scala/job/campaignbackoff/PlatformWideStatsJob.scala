package job.campaignbackoff

import com.thetradedesk.plutus.data.transform.campaignbackoff.PlatformWideStatsTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object PlatformWideStatsJob {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 200)

  val prometheus = new PrometheusClient("CampaignBackoff", "PlatformWideStatsJob")
  val jobDurationGauge = prometheus.createGauge("platformwidestats_run_time_seconds", "Job execution time in seconds")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    PlatformWideStatsTransform.transform(date, fileCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
