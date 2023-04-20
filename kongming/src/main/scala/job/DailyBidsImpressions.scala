package job

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicySnapshotDataset, BidsImpressionsSchema, DailyBidsImpressionsDataset, UnifiedAdGroupDataSet}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object DailyBidsImpressions {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, "DailyBidsImpressions")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicy = AdGroupPolicySnapshotDataset().readDataset(date)

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date)

    val dailyBidsImpressions = multiLevelJoinWithPolicy[BidsImpressionsSchema](bidsImpressions, adGroupPolicy, joinType = "left_semi")

    val dailyBidsImpressionsRows = DailyBidsImpressionsDataset().writePartition(dailyBidsImpressions, date, Some(400))

    outputRowsWrittenGauge.labels("DailyBidsImpressionsDataset").set(dailyBidsImpressionsRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()
    spark.stop()
  }
}
