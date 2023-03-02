package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicySnapshotDataset, DailyBidRequestDataset}
import com.thetradedesk.kongming.transform.BidRequestTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate

object DailyBidRequest {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, "DailyBidRequest")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](BidsImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicy = AdGroupPolicySnapshotDataset().readDataset(date)

    val filteredBidRequestDS = BidRequestTransform.dailyTransform(
      date,
      bidsImpressions,
      adGroupPolicy
    )(prometheus)

    val dailyBrRows = DailyBidRequestDataset().writePartition(filteredBidRequestDS, date, Some(100))

    outputRowsWrittenGauge.labels("DailyBidRequestDataset").set(dailyBrRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
