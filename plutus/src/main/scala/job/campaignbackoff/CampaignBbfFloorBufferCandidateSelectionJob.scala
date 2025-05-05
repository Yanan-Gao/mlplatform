package job.campaignbackoff

import com.thetradedesk.plutus.data.transform.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionTransform
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object CampaignBbfFloorBufferCandidateSelectionJob {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 10)
  val underdeliveryFraction = config.getDouble("underdeliveryFraction", 0.02)
  val throttleThreshold = config.getDouble("throttle", 0.8)
  val rollbackUnderdeliveryFraction = config.getDouble("rollbackUnderdeliveryFraction", 0.05)
  val rollbackThrottleThreshold = config.getDouble("rollbackThrottle", 0.8)
  val openMarketShare = config.getDouble("openMarketShare", 0.01)
  val openPathShare = config.getDouble("openPathShare", 0.01)

  val prometheus = new PrometheusClient("CampaignBackoff", "CampaignBbfFloorBufferSelection")
  val jobDurationGauge = prometheus.createGauge("campaign_bbf_floor_buffer_selection_job_run_time_seconds", "Job execution time in seconds")
  val numFloorBufferRowsWritten = prometheus.createGauge("camapign_bbf_floor_buffer_num_rows", "Number of total rows in floor buffer file (or campaigns)")
  val numFloorBufferRollbackRowsWritten = prometheus.createGauge("camapign_bbf_floor_buffer_rollback_num_rows", "Number of total rows rolled back (or campaigns)")
  val floorBufferMetrics = prometheus.createGauge("camapign_bbf_floor_buffer_count_by_val", "Number of total rows for each floor buffer", "FloorBuffer")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    CampaignBbfFloorBufferCandidateSelectionTransform.transform(date = date,
      fileCount = fileCount,
      underdeliveryFraction = underdeliveryFraction,
      throttleThreshold = throttleThreshold,
      openMarketShare = openMarketShare,
      openPathShare = openPathShare,
      rollbackUnderdeliveryFraction = rollbackUnderdeliveryFraction,
      rollbackThrottleThreshold = rollbackThrottleThreshold
    )

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    TTDSparkContext.spark.stop()
  }

}
