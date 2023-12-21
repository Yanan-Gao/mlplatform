package job

import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.{date, getMinimalPolicy, multiLevelJoinWithPolicy, partCount}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._

import org.apache.spark.sql.functions._

import java.time.LocalDate

object ProdToExperimentBackfiller extends KongmingBaseJob {
  object Config {
    //TODO: ideally policy level lookback would be based on attribution window. This below value will set a cap.
    //TODO: based on research yuehan did: https://atlassian.thetradedesk.com/jira/browse/AUDAUTO-284 plus a buffer
    var outExperiment = config.getString("outExperiment", default="research")
  }

  override def jobName: String = "ProdToExperimentBackfiller"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val policy = AdGroupPolicyDataset(Some(Config.outExperiment)).readDate(date)
    val mapping = AdGroupPolicyMappingDataset(Some(Config.outExperiment)).readDate(date)

    val minimalPolicy = getMinimalPolicy(policy, mapping)
    val srcImps = DailyBidsImpressionsDataset().readDate(date)
    val dstImps = multiLevelJoinWithPolicy[BidsImpressionsSchema](srcImps, minimalPolicy, "left_semi")
    val impRowCount = DailyBidsImpressionsDataset(Some(Config.outExperiment)).writePartition(dstImps, date, Some(partCount.DailyBidsImpressions))

    val srcReqs = DailyBidRequestDataset().readDate(date)
    val dstReqs = srcReqs.join(policy.select("DataAggKey", "DataAggValue").distinct(), Seq("DataAggKey", "DataAggValue"), "left_semi")
      .selectAs[DailyBidRequestRecord]
    val reqRowCount = DailyBidRequestDataset(Some(Config.outExperiment)).writePartition(dstReqs, date, Some(partCount.DailyBidRequest))

    val srcConvs = DailyConversionDataset().readDate(date)
    val dstConvs = srcConvs.join(policy.select("ConfigKey", "ConfigValue"), Seq("ConfigKey", "ConfigValue"), "left_semi")
      .selectAs[DailyConversionDataRecord]
    val convRowCount = DailyConversionDataset(Some(Config.outExperiment)).writePartition(dstConvs, date, Some(partCount.DailyConversion))

    val srcNegs = DailyNegativeSampledBidRequestDataSet().readDate(date)
    val dstNegs = srcNegs.join(policy.filter('DataAggKey === lit("CampaignId")).select('DataAggValue.as("CampaignId"), lit(1).as("PickedCampaign")), Seq("CampaignId"), "left")
      .join(policy.filter('DataAggKey === lit("AdvertiserId")).select('DataAggValue.as("AdvertiserId"), lit(1).as("PickedAdvertiser")), Seq("AdvertiserId"), "left")
      .filter('PickedCampaign.isNotNull || 'PickedAdvertiser.isNotNull)
      .selectAs[DailyNegativeSampledBidRequestRecord]
    val negRowCount = DailyNegativeSampledBidRequestDataSet(Some(Config.outExperiment)).writePartition(dstNegs, date, Some(partCount.DailyNegativeSampledBidRequest))

    val srcPos = DailyPositiveBidRequestDataset().readDate(date)
    val dstPos = srcPos.join(policy.select("ConfigKey", "ConfigValue"), Seq("ConfigKey", "ConfigValue"), "left_semi")
      .selectAs[DailyPositiveLabelRecord]
    val posRowCount = DailyPositiveBidRequestDataset(Some(Config.outExperiment)).writePartition(dstPos, date, Some(partCount.DailyPositiveBidRequest))

    val srcPosSummary = DailyPositiveCountSummaryDataset().readDate(date)
    val dstPosSummary = srcPosSummary.join(mapping.select("AdvertiserId").distinct, Seq("AdvertiserId"), "left_semi")
      .selectAs[DailyPositiveCountSummaryRecord]
    val posSummaryRowCount = DailyPositiveCountSummaryDataset(Some(Config.outExperiment)).writePartition(dstPosSummary, date, Some(partCount.DailyPositiveCountSummary))

    Array(impRowCount, reqRowCount, convRowCount, negRowCount, posRowCount, posSummaryRowCount)
  }
}
