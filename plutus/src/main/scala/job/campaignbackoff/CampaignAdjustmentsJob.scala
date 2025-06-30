package job.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignbackoff.PlutusCampaignAdjustmentsDataset
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.MergedCampaignFloorBufferDataset
import com.thetradedesk.plutus.data.transform.campaignbackoff.{HadesCampaignAdjustmentsTransform, HadesCampaignBufferAdjustmentsTransform, MergeCampaignBackoffAdjustments, PlutusCampaignAdjustmentsTransform}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.opentelemetry.OtelClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object CampaignAdjustmentsJob {
  val date = config.getDateRequired("date")
  val testSplit = config.getDoubleOption("testSplit")
  val updateAdjustmentsVersion = config.getStringRequired("updateAdjustmentsVersion")
  val fileCount = config.getInt("fileCount", 10)
  val underdeliveryThreshold = config.getDouble("underdeliveryThreshold", 0.1)

  val otelClient = new OtelClient("CampaignBackoff", "CampaignAdjustmentsJob")
  val jobDurationGauge = otelClient.createGauge("campaignadjustments_run_time_seconds", "Job execution time in seconds")
  val numRowsWritten = otelClient.createGauge("campaignadjustments_num_rows", "Number of total rows in file (or campaigns)")
  val campaignCounts = otelClient.createGauge("campaignadjustments_plutus_campaign_count", "Number of new, removed, worse, and DA campaigns added to file")
//  val testControlSplit = otelClient.createGauge("campaignadjustments_campaign_split", "Rate of control vs test campaigns", labelNames = "test")
  val hadesCampaignCounts = otelClient.createGauge("campaignadjustments_hades_campaign_count", "Number of identified Hades problem campaigns")
  val hadesMetrics = otelClient.createGauge("campaignadjustments_hades_campaign_types", "Different adjustment types")
  val hadesBackoffV3Metrics = otelClient.createGauge("campaignadjustments_hades_backoffv3_campaign_types", "Different adjustment types")


  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    val campaignFloorBufferData = MergedCampaignFloorBufferDataset.readDate(date, envForReadInternal)
    val campaignAdjustmentsPacingData = PlutusCampaignAdjustmentsDataset.readLatestDataUpToIncluding(date.minusDays(1), envForReadInternal)

    val plutusCampaignAdjustmentsDataset = PlutusCampaignAdjustmentsTransform.transform(date, updateAdjustmentsVersion, fileCount)
    val hadesCampaignBufferAdjustmentsDataset  = HadesCampaignBufferAdjustmentsTransform.transform(date, underdeliveryThreshold, fileCount, campaignFloorBufferData, campaignAdjustmentsPacingData)
    MergeCampaignBackoffAdjustments.transform(plutusCampaignAdjustmentsDataset, campaignFloorBufferData, hadesCampaignBufferAdjustmentsDataset)

    jobDurationGaugeTimer.setDuration()
    otelClient.pushMetrics()
    TTDSparkContext.spark.stop()
  }
}
