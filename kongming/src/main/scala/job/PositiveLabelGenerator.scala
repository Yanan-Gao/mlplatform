package job


import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressions
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.BidRequestTransform
import com.thetradedesk.kongming.transform.PositiveLabelDailyTransform
import com.thetradedesk.logging.Logger
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.Testing
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import org.apache.spark.sql.functions._

import java.time.LocalDate

/**
 * object to join conversion data with last touches in bidrequest dataset.
 */
object PositiveLabelGenerator extends Logger{
  //TODO: ideally policy level lookback would be based on attribution window. This below value will set a cap.
  //TODO: based on research yuehan did: https://atlassian.thetradedesk.com/jira/browse/AUDAUTO-284 plus a buffer
  val bidLookback = config.getInt("bidLookback", default = 20)
  //TODO: longer conv lookback will add more white space. For now, we settle on daily processing.
  val convLookback = config.getInt("convLookback", default=1)

  def main(args: Array[String]): Unit = {

    //TODO: need to consider attributed event here? consider in an implicit way by taking into last imp into account
    //maybe we blend in a light translation layer here? for attributed events?
    //keep the last n touches on bidrequest and 1 touch on bidfeedback and 1 touch on click if click is there.
    //indicating long vs short window and use weight to differenciate them.

    //load config datasets
    val prometheus = new PrometheusClient(KongmingApplicationName, "PositiveLabeling")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    // read master policy
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, true)

    // resolve for maxLookback
    val maxPolicyLookbackInDays = adGroupPolicy.agg(max($"DataLookBack")).head.getAs[Int](0)
    val lookback = math.min(maxPolicyLookbackInDays, bidLookback) - 1 //the -1 is to account for the given date is partial

    // previous multiday data
    val rawMultiDayBidRequestDS = DailyBidRequestDataset().readRange(date.minusDays(lookback+1), date)

    // single day data
    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)
    val dailyConversionDS = DailyConversionDataset().readDate(date).cache
    val sameDayPositiveBidRequestDS = PositiveLabelDailyTransform.intraDayConverterNTouchesTransform(
      bidsImpressionFilterByPolicy,
      adGroupPolicy,
      dailyConversionDS,
      adGroupDS
    )(prometheus)

    //join conversion and unioned dataset to get the final result
    //Note: join conversion first and then do rank will speed up calculation.
    val multiDayPositiveBidRequestDS = PositiveLabelDailyTransform.multiDayConverterTransform(
      rawMultiDayBidRequestDS,
      dailyConversionDS,
      adGroupPolicy
    )(prometheus)

    //union same day bidrequest with previous many days
    val unionedPositiveBidRequestDS = sameDayPositiveBidRequestDS.union(multiDayPositiveBidRequestDS)

    val positiveLabelDS = PositiveLabelDailyTransform.positiveLabelAggTransform(unionedPositiveBidRequestDS, adGroupPolicy)

    val posDataset = DailyPositiveBidRequestDataset()
    val dailyPositiveBrRows = posDataset.writePartition(positiveLabelDS, date, Some(100))

    // Hackity-hack
    val writeEnv = environment
    if (posDataset.readRoot != posDataset.writeRoot) {
      environment = Testing
    }

    val rereadPos = DailyPositiveBidRequestDataset().readDate(date)
    if (posDataset.readRoot != posDataset.writeRoot) {
      environment = writeEnv
    }

    val positiveSummary = PositiveLabelDailyTransform.countDataAggGroupPositives(rereadPos)

    DailyPositiveCountSummaryDataset().writePartition(positiveSummary, date, Some(50))

    outputRowsWrittenGauge.labels("DailyPositiveBidRequestDataset").set(dailyPositiveBrRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
