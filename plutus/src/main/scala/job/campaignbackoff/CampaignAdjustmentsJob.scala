package job.campaignbackoff

import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignAdjustmentsTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object CampaignAdjustmentsJob {
  val date = config.getDateRequired("date")
  val testSplit = config.getDoubleOption("testSplit")
  val updateAdjustmentsVersion = config.getStringRequired("updateAdjustmentsVersion")
  val fileCount = config.getInt("fileCount", 200)

  val prometheus = new PrometheusClient("CampaignBackoff", "CampaignAdjustmentsJob")
  val jobDurationGauge = prometheus.createGauge("campaignadjustments_run_time_seconds", "Job execution time in seconds")
  val numRowsWritten = prometheus.createGauge("campaignadjustments_num_rows", "Number of total rows in file (or campaigns)")
  val campaignCounts = prometheus.createGauge("campaignadjustments_campaign_count", "Number of new, removed, worse, and DA campaigns added to file", labelNames = "status")
  val testControlSplit = prometheus.createGauge("campaignadjustments_campaign_split", "Rate of control vs test campaigns", labelNames = "test")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    CampaignAdjustmentsTransform.transform(date, testSplit, updateAdjustmentsVersion, fileCount)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
