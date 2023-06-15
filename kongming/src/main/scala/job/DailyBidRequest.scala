package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyBidsImpressionsDataset, DailyBidRequestDataset}
import com.thetradedesk.kongming.transform.BidRequestTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import java.time.LocalDate

object DailyBidRequest {
  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("DailyBidRequest"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date)
    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    val filteredBidRequestDS = BidRequestTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      adGroupPolicy
    )(prometheus)

    val dailyBrRows = DailyBidRequestDataset().writePartition(filteredBidRequestDS, date, Some(5000))

    outputRowsWrittenGauge.labels("DailyBidRequestDataset").set(dailyBrRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
