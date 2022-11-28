package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, AdGroupPolicySnapshotDataset, AdGroupPolicySnapshotRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.prometheus.PrometheusClient

object DailyAdGroupPolicySnapshot {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, "DailyAdGroupPolicySnapshot")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataFrame(adGroupPolicyHardCodedDate).selectAs[AdGroupPolicySnapshotRecord]

    val dailyAdGroupPolicyRows = AdGroupPolicySnapshotDataset().writePartition(adGroupPolicy, date, Some(100))


    outputRowsWrittenGauge.labels("DailyAdGroupPolicySnapshotDataset").set(dailyAdGroupPolicyRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
