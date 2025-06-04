package job.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsPacingSchema, PlutusCampaignAdjustmentsDataset}
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.{CampaignFloorBufferSchema, MergedCampaignFloorBufferDataset}
import com.thetradedesk.plutus.data.transform.campaignbackoff.{HadesCampaignAdjustmentsTransform, HadesCampaignBufferAdjustmentsTransform, MergeCampaignBackoffAdjustments, PlutusCampaignAdjustmentsTransform}
import com.thetradedesk.plutus.data.transform.campaignfloorbuffer.MergeCampaignFloorBufferTransform.readMergedFloorBufferData
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object CampaignAdjustmentsJob {
  val date = config.getDateRequired("date")
  val testSplit = config.getDoubleOption("testSplit")
  val updateAdjustmentsVersion = config.getStringRequired("updateAdjustmentsVersion")
  val fileCount = config.getInt("fileCount", 10)
  val underdeliveryThreshold = config.getDouble("underdeliveryThreshold", 0.1)

  val prometheus = new PrometheusClient("CampaignBackoff", "CampaignAdjustmentsJob")
  val jobDurationGauge = prometheus.createGauge("campaignadjustments_run_time_seconds", "Job execution time in seconds")
  val numRowsWritten = prometheus.createGauge("campaignadjustments_num_rows", "Number of total rows in file (or campaigns)")
  val campaignCounts = prometheus.createGauge("campaignadjustments_plutus_campaign_count", "Number of new, removed, worse, and DA campaigns added to file", labelNames = "status")
//  val testControlSplit = prometheus.createGauge("campaignadjustments_campaign_split", "Rate of control vs test campaigns", labelNames = "test")
  val hadesCampaignCounts = prometheus.createGauge("campaignadjustments_hades_campaign_count", "Number of identified Hades problem campaigns", labelNames = "status")
  val hadesMetrics = prometheus.createGauge("campaignadjustments_hades_campaign_types", "Different adjustment types", "CampaignType", "Pacing", "OptOut", "Quantile")

  val hadesBackoffV3Metrics = prometheus.createGauge("campaignadjustments_hades_backoffv3_campaign_types", "Different adjustment types", "CampaignType", "Pacing", "OptOut", "Quantile", "Backoff")


  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    val campaignFloorBufferData = MergedCampaignFloorBufferDataset.readDate(date, envForReadInternal)
    val campaignAdjustmentsPacingData = PlutusCampaignAdjustmentsDataset.readLatestDataUpToIncluding(date.minusDays(1), envForReadInternal)

    val plutusCampaignAdjustmentsDataset = PlutusCampaignAdjustmentsTransform.transform(date, updateAdjustmentsVersion, fileCount)
    val hadesCampaignAdjustmentsDataset  = HadesCampaignAdjustmentsTransform.transform(date, testSplit, underdeliveryThreshold, fileCount, campaignFloorBufferData)
    val hadesCampaignBufferAdjustmentsDataset  = HadesCampaignBufferAdjustmentsTransform.transform(date, underdeliveryThreshold, fileCount, campaignFloorBufferData, campaignAdjustmentsPacingData)
    MergeCampaignBackoffAdjustments.transform(plutusCampaignAdjustmentsDataset, hadesCampaignAdjustmentsDataset, campaignFloorBufferData, hadesCampaignBufferAdjustmentsDataset)

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
