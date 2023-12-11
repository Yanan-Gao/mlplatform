package job

import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.PositiveLabelDailyTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

/**
 * object to join conversion data with last touches in bidrequest dataset.
 */
object PositiveLabelGenerator extends KongmingBaseJob {
  //TODO: ideally policy level lookback would be based on attribution window. This below value will set a cap.
  //TODO: based on research yuehan did: https://atlassian.thetradedesk.com/jira/browse/AUDAUTO-284 plus a buffer
  val bidLookback = config.getInt("bidLookback", default = 20)
  //TODO: longer conv lookback will add more white space. For now, we settle on daily processing.
  val convLookback = config.getInt("convLookback", default=1)

  override def jobName: String = "PositiveLabeling"

  def generateData(date: LocalDate)(implicit prometheus: PrometheusClient): Dataset[DailyPositiveLabelRecord] = {
    //TODO: need to consider attributed event here? consider in an implicit way by taking into last imp into account
    //maybe we blend in a light translation layer here? for attributed events?
    //keep the last n touches on bidrequest and 1 touch on bidfeedback and 1 touch on click if click is there.
    //indicating long vs short window and use weight to differenciate them.

    // read master policy
    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache
    val adGroupPolicyMapping = AdGroupPolicyMappingDataset().readDate(date)

    // resolve for maxLookback
    val maxPolicyLookbackInDays = adGroupPolicy.agg(max($"DataLookBack")).head.getAs[Int](0)
    val lookback = math.min(maxPolicyLookbackInDays, bidLookback) - 1 //the -1 is to account for the given date is partial

    // previous multiday data
    val rawMultiDayBidRequestDS = DailyBidRequestDataset().readRange(date.minusDays(lookback+1), date, isInclusive=false).selectAs[DailyBidRequestRecord]
    // single day data
    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    // Do not generate data for busted campaigns (campaigns with absolutely no bid requests)
    val dailyConversionDS = DailyConversionDataset().readDate(date)

    val preFilteredPolicy = rawMultiDayBidRequestDS.select("AdGroupId").distinct
      .join(adGroupPolicyMapping.select("AdGroupId", "CampaignId"), Seq("AdGroupId"), "inner").select("CampaignId")
      .union(bidsImpressionFilterByPolicy.select("CampaignId")).distinct
      .join(adGroupPolicyMapping.select("CampaignId", "ConfigKey", "ConfigValue").distinct, Seq("CampaignId"), "inner")
      .join(adGroupPolicy, Seq("ConfigKey", "ConfigValue"), "inner")
      .selectAs[AdGroupPolicyRecord]

    val sameDayPositiveBidRequestDS = PositiveLabelDailyTransform.intraDayConverterNTouchesTransform(
      bidsImpressionFilterByPolicy,
      preFilteredPolicy,
      dailyConversionDS,
    )(getPrometheus)

    //join conversion and unioned dataset to get the final result
    //Note: join conversion first and then do rank will speed up calculation.
    val multiDayPositiveBidRequestDS = PositiveLabelDailyTransform.multiDayConverterTransform(
      rawMultiDayBidRequestDS,
      dailyConversionDS,
      preFilteredPolicy
    )(getPrometheus)

    //union same day bidrequest with previous many days
    val unionedPositiveBidRequestDS = sameDayPositiveBidRequestDS.union(multiDayPositiveBidRequestDS)
    PositiveLabelDailyTransform.positiveLabelAggTransform(unionedPositiveBidRequestDS, preFilteredPolicy)
  }

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val positiveLabelDS = generateData(date)(getPrometheus)

    val dailyPositiveBrRows = DailyPositiveBidRequestDataset().writePartition(positiveLabelDS, date, Some(partCount.DailyPositiveBidRequest))

    config.overridePathVariable("ttd.DailyPositiveBidRequestDataset.isInChain", "true")
    val pos = DailyPositiveBidRequestDataset().readDate(date)

    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, isInclusive = true)
    val positiveSummary = PositiveLabelDailyTransform.countDataAggGroupPositives(pos, adGroupDS)

    DailyPositiveCountSummaryDataset().writePartition(positiveSummary, date, Some(partCount.DailyPositiveCountSummary))

    Array(dailyPositiveBrRows)

  }
}
