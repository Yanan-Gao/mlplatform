package job.campaignbackoff

import com.thetradedesk.plutus.data.{envForRead, envForReadInternal}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignFlightDataset, CampaignThrottleMetricDataset}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{Campaign, CampaignFlightData, PacingData, bucketCount, getTestBucketUDF, platformWideBuffer}
import com.thetradedesk.plutus.data.transform.campaignfloorbuffer.{CampaignFloorBufferCandidateSelectionTransform, MergeCampaignFloorBufferTransform}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.functions.{col, lit}

object CampaignBbfFloorBufferCandidateSelectionJob {
  val date = config.getDateRequired("date")
  val fileCount = config.getInt("fileCount", 10)
  val testSplit = config.getDoubleOption("testSplit")
  val underdeliveryFraction = config.getDouble("underdeliveryFraction", 0.02)
  val throttleThreshold = config.getDouble("throttle", 0.8)
  val rollbackUnderdeliveryFraction = config.getDouble("rollbackUnderdeliveryFraction", 0.05)
  val rollbackThrottleThreshold = config.getDouble("rollbackThrottle", 0.8)
  val openMarketShare = config.getDouble("openMarketShare", 0.01)
  val openPathShare = config.getDouble("openPathShare", 0.01)

  //Read configs for bucket range in the experiment
  val expBucketRangeStart = config.getInt("expBucketRangeStart", 0)
  val expBucketRangeEnd = config.getInt("expBucketRangeEnd", 0)
  val expFloorBuffer = config.getDouble("expFloorBuffer", platformWideBuffer)

  val prometheus = new PrometheusClient("CampaignBackoff", "CampaignBbfFloorBufferCandidateSelectionJob")
  val jobDurationGauge = prometheus.createGauge("campaign_bbf_floor_buffer_selection_job_run_time_seconds", "Job execution time in seconds")
  val numFloorBufferRowsWritten = prometheus.createGauge("campaign_bbf_floor_buffer_num_rows", "Number of total rows in floor buffer file (or campaigns)")
  val numAdhocFloorBufferRowsWritten = prometheus.createGauge("campaign_bbf_adhoc_floor_buffer_num_rows", "Number of total rows in adhoc floor buffer file (or campaigns)")
  val numFloorBufferRollbackRowsWritten = prometheus.createGauge("campaign_bbf_floor_buffer_rollback_num_rows", "Number of total rows rolled back (or campaigns)")
  val floorBufferMetrics = prometheus.createGauge("campaign_bbf_floor_buffer_count_by_val", "Number of total rows for each floor buffer", "FloorBuffer")

  def main(args: Array[String]): Unit = {
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()

    // Both the below transforms need campaigns within the Hades test bucket range
    val liveHadesTestCampaigns = CampaignFlightDataset.readDate(date, envForReadInternal)
      .filter($"IsCurrent" === 1 && $"StartDateInclusiveUTC" <= date && $"EndDateExclusiveUTC" >= date)
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") < (lit(bucketCount) * testSplit.getOrElse(1.0)))
      .selectAs[CampaignFlightData].distinct()
      .cache()

    val daCampaigns = CampaignThrottleMetricDataset.readDate(date, envForRead)
      .selectAs[PacingData]
      .filter(col("IsValuePacing"))
      .cache()

    val hadesTestDACampaigns = liveHadesTestCampaigns
      .join(daCampaigns, Seq("CampaignId"), "inner")
      .selectAs[CampaignFlightData].distinct()
      .cache()

    // Generate todays automated campaign floor buffer data
    val todaysCampaignFloorBufferData = CampaignFloorBufferCandidateSelectionTransform.transform(
      date = date,
      fileCount = fileCount,
      underdeliveryFraction = underdeliveryFraction,
      throttleThreshold = throttleThreshold,
      openMarketShare = openMarketShare,
      openPathShare = openPathShare,
      rollbackUnderdeliveryFraction = rollbackUnderdeliveryFraction,
      rollbackThrottleThreshold = rollbackThrottleThreshold,
      liveHadesTestDACampaigns = hadesTestDACampaigns
    )

    val liveExperimentCampaigns = hadesTestDACampaigns
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") >= expBucketRangeStart && col("TestBucket") < expBucketRangeEnd)
      .selectAs[CampaignFlightData].distinct()
      .cache()

    MergeCampaignFloorBufferTransform.transform(
      date = date,
      fileCount = fileCount,
      expFloorBuffer = expFloorBuffer,
      todaysCampaignFloorBufferData = todaysCampaignFloorBufferData,
      liveExperimentCampaigns = liveExperimentCampaigns
    )

    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    TTDSparkContext.spark.stop()
  }

}
