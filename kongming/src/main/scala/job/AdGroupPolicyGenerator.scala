package job

import com.thetradedesk.geronimo.shared.shiftModUdf
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupDataSet, AdGroupPolicyMappingDataset, AdGroupPolicyMappingRecord, AdGroupPolicyDataset, AdGroupPolicyRecord, AdvertiserDataSet, CampaignConversionReportingColumnDataSet, CampaignDataSet, CampaignFlightDataSet, DailyPositiveCountSummaryDataset, PartnerDataSet}
import com.thetradedesk.kongming.transform.TrainSetFeatureMappingTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object AdGroupPolicyGenerator {
  object Config {
    var GlobalMinDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMinDailyConvCount", 1)
    var GlobalMaxDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMaxDailyConvCount", 50000)
    var GlobalLastTouchCount = config.getInt("AdGroupPolicyGenerator.GlobalLastTouchCount", 5)
    var GlobalDataLookBack = config.getInt("AdGroupPolicyGenerator.GlobalDataLookBack", 15)
    var GlobalPositiveSamplingRate = config.getDouble("AdGroupPolicyGenerator.GlobalPositiveSamplingRate", 1)
    var GlobalNegativeSamplingMethod = config.getString("AdGroupPolicyGenerator.GlobalNegativeSamplingMethod", "Constant")
    var GlobalCrossDeviceConfidenceLevel = config.getDouble("AdGroupPolicyGenerator.GlobalCrossDeviceConfidenceLevel", 0.01)
    var GlobalFetchOlderTrainingData = config.getBoolean("AdGroupPolicyGenerator.GlobalFetchOlderTrainingData", false)
    var GlobalOldTrainingDataEpoch = config.getInt("AdGroupPolicyGenerator.GlobalOldTrainingDataEpoch", 2)
    /* This setting applies to both the dynamic LastTouchCount and DataAggKey. The dynamic logic will only be applied to
     * "established" ad groups, or ad groups that are at least PositivesCountWarmUpdays old. If this is set to 0,
     * dynamic policy settings that relies on the positive pool size will NOT be applied.
     */
    var PositivesCountWarmUpDays = config.getInt("AdGroupPolicyGenerator.PositivesCountWarmUpDays", 7)

    /* If the data aggregation group has less than LastTouchCountRareThreshold rows, set the LastTouchCount to
     * LastTouchcountRareLastTouchCount for that policy config item. Otherwise, use the default value of GlobalLastTouchCount.
     * The dynamic logic will not apply if the data aggregation level is changed to ensure that only one change is applied
     * to the policy table at a time.
     */
    var LastTouchCountRareThreshold = config.getInt("AdGroupPolicyGenerator.LastTouchCountRareThreshold", 200)
    var LastTouchCountRareLastTouchCount = config.getInt("AdGroupPolicyGenerator.LastTouchCountRareLastTouchCount", 10)

    /* If an ad group has less than AggregateOnAdvertiserThreshold positive samples, aggregate over AdvertiserId. Otherwise,
     * aggregate over CampaignId.
     */
    var AggregateOnAdvertiserThreshold = config.getInt("AdGroupPolicyGenerator.AggregateOnAdvertiserThreshold", 20)

    /* Controls whether the previous day's policy table is read to maintain stability in the ConfigValue chosen when there
     * are multiple active CPA ad groups in the data agg group. This should be true for production but can be turned off
     * to allow for the data generation to be parallelised during testing/backfilling for some test cases.
     */
    var TryMaintainConfigValue = config.getBoolean("AdGroupPolicyGenerator.TryMaintainConfigValue", true)
  }

  class ConditionalRule(condition: Column, value: Column) {
    def get(): Column = {
      when(condition, value)
    }

    def get(previous: Column) = {
      previous.when(condition, value)
    }
  }

  class CategoricalRule[T](categoryField: Symbol, categoryValue: T, value: Column)
    extends ConditionalRule(categoryField === lit(categoryValue), value)

  class ConditionalSetting(firstRule: ConditionalRule, otherRules: ConditionalRule*) {
    def get(): Column = {
      var output = firstRule.get()

      for (rule <- otherRules) {
        output = rule.get(output)
      }

      output
    }
  }

  case class PolicyTableAdGroup(AdGroupId: String,
                                CampaignId: String,
                                AdvertiserId: String,
                                AttributionClickLookbackWindowInSeconds: Int,
                                AttributionImpressionLookbackWindowInSeconds: Int
                               )

  case class StaticPolicyTable(AdGroupId: String,
                               CampaignId: String,
                               AdvertiserId: String,
                               AttributionClickLookbackWindowInSeconds: Int,
                               AttributionImpressionLookbackWindowInSeconds: Int,
                               MinDailyConvCount: Int,
                               MaxDailyConvCount: Int,
                               DataLookBack: Int,
                               PositiveSamplingRate: Double,
                               NegativeSamplingMethod: String,
                               CrossDeviceConfidenceLevel: Double,
                               FetchOlderTrainingData: Boolean,
                               OldTrainingDataEpoch: Int
                              )

  case class PolicyTableWithDailyCounts(AdGroupId: String,
                                        CampaignId: String,
                                        AdvertiserId: String,
                                        AttributionClickLookbackWindowInSeconds: Int,
                                        AttributionImpressionLookbackWindowInSeconds: Int,
                                        MinDailyConvCount: Int,
                                        MaxDailyConvCount: Int,
                                        DataLookBack: Int,
                                        PositiveSamplingRate: Double,
                                        NegativeSamplingMethod: String,
                                        CrossDeviceConfidenceLevel: Double,
                                        FetchOlderTrainingData: Boolean,
                                        OldTrainingDataEpoch: Int,
                                        established: Boolean,
                                        DailyAdGroupCount: Double,
                                        DailyCampaignCount: Double,
                                        DailyAdvertiserCount: Double
                                       )

  case class PolicyTableWithAggregationLevel(AdGroupId: String,
                                             CampaignId: String,
                                             AdvertiserId: String,
                                             AttributionClickLookbackWindowInSeconds: Int,
                                             AttributionImpressionLookbackWindowInSeconds: Int,
                                             MinDailyConvCount: Int,
                                             MaxDailyConvCount: Int,
                                             DataLookBack: Int,
                                             PositiveSamplingRate: Double,
                                             NegativeSamplingMethod: String,
                                             CrossDeviceConfidenceLevel: Double,
                                             FetchOlderTrainingData: Boolean,
                                             OldTrainingDataEpoch: Int,
                                             established: Boolean,
                                             DailyAdGroupCount: Double,
                                             DailyCampaignCount: Double,
                                             DailyAdvertiserCount: Double,
                                             DataAggKey: String,
                                             DataAggValue: String,
                                             DailyDataAggGroupCount: Double
                                            )

  case class PolicyTableWithLastTouchCount(AdGroupId: String,
                                           CampaignId: String,
                                           AdvertiserId: String,
                                           AttributionClickLookbackWindowInSeconds: Int,
                                           AttributionImpressionLookbackWindowInSeconds: Int,
                                           MinDailyConvCount: Int,
                                           MaxDailyConvCount: Int,
                                           DataLookBack: Int,
                                           PositiveSamplingRate: Double,
                                           NegativeSamplingMethod: String,
                                           CrossDeviceConfidenceLevel: Double,
                                           FetchOlderTrainingData: Boolean,
                                           OldTrainingDataEpoch: Int,
                                           established: Boolean,
                                           DailyAdGroupCount: Double,
                                           DailyCampaignCount: Double,
                                           DailyAdvertiserCount: Double,
                                           DataAggKey: String,
                                           DataAggValue: String,
                                           DailyDataAggGroupCount: Double,
                                           LastTouchCount: Int
                                          )

  def addAggregationLevel(policy: Dataset[PolicyTableWithDailyCounts]): Dataset[PolicyTableWithAggregationLevel] = {
    policy
      .withColumn("DataAggKey", when('established && 'DailyCampaignCount < lit(Config.AggregateOnAdvertiserThreshold), lit("AdvertiserId")).otherwise("CampaignId"))
      .withColumn("DataAggValue", new ConditionalSetting(
        new CategoricalRule[String]('DataAggKey, "CampaignId", 'CampaignId),
        new CategoricalRule[String]('DataAggKey, "AdGroupId", 'AdGroupId),
        new CategoricalRule[String]('DataAggKey, "AdvertiserId", 'AdvertiserId)).get())
      .withColumn("DailyDataAggGroupCount", when('DataAggKey === lit("AdGroupId"), 'DailyAdGroupCount).when('DataAggKey === lit("CampaignId"), 'DailyCampaignCount).when('DataAggKey === lit("AdvertiserId"), 'DailyAdvertiserCount).otherwise(0))
      .selectAs[PolicyTableWithAggregationLevel]
  }

  def addLastTouchCount(policy: Dataset[PolicyTableWithAggregationLevel], yesterdaysPolicy: Dataset[AdGroupPolicyRecord]): Dataset[PolicyTableWithLastTouchCount] = {
    policy
      .join(yesterdaysPolicy.select('DataAggKey, 'DataAggValue, 'DataAggKey.as("DataAggKeyYesterday")), Seq("DataAggKey", "DataAggValue"), "left")
      .withColumn("LastTouchCount", when('established && ('DailyDataAggGroupCount < lit(Config.LastTouchCountRareThreshold)) && 'DataAggKeyYesterday === 'DataAggKey,
          Config.LastTouchCountRareLastTouchCount).otherwise(
          Config.GlobalLastTouchCount
        ))
      .selectAs[PolicyTableWithLastTouchCount]
  }

  def removeSuperfluousAdGroups(date: LocalDate, policy: Dataset[AdGroupPolicyRecord], yesterdaysPolicy: Dataset[AdGroupPolicyRecord], adgroups: Dataset[PolicyTableAdGroup]): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    val yesterdaysPolicyRenamd = yesterdaysPolicy.select("ConfigKey", "ConfigValue", "DataAggKey", "DataAggValue")
      .withColumnRenamed("ConfigKey", "PolicyConfigKey")
      .withColumnRenamed("ConfigValue", "PolicyConfigValue")

    val decisions = policy.join(yesterdaysPolicyRenamd, Seq("DataAggKey", "DataAggValue"), "left")
      .withColumn("rank", rank().over(Window.partitionBy('DataAggKey, 'DataAggValue).orderBy('ConfigKey, 'ConfigValue)))
      .withColumn("picked", when('PolicyConfigKey.isNotNull && 'PolicyConfigValue.isNotNull,
        'ConfigKey === 'PolicyConfigKey && 'ConfigValue === 'PolicyConfigValue).otherwise(
        'rank === lit(1)
      ))

    val newPolicyTable = decisions.filter('picked)
      .selectAs[AdGroupPolicyRecord]
    val policyTableMapping = decisions
      .withColumnRenamed("ConfigValue", "AdGroupId").select("AdGroupId", "DataAggKey", "DataAggValue")
      .join(newPolicyTable, Seq("DataAggKey", "DataAggValue"), "left")
      .join(adgroups.select("AdGroupId", "CampaignId", "AdvertiserId"), Seq("AdGroupId"), "left")
      .withColumn("AdGroupIdInt", shiftModUdf(xxhash64('AdGroupId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("AdGroupId"))))
      .withColumn("CampaignIdInt", shiftModUdf(xxhash64('CampaignId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("CampaignId"))))
      .withColumn("AdvertiserIdInt", shiftModUdf(xxhash64('AdvertiserId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("AdvertiserId"))))
      .selectAs[AdGroupPolicyMappingRecord]
      .distinct()

    (newPolicyTable, policyTableMapping)
  }

  // Set the policy for everything that can be determined before the aggregation level
  def addPositiveCountSummary(policy: Dataset[StaticPolicyTable]): Dataset[PolicyTableWithDailyCounts] = {
    if (Config.PositivesCountWarmUpDays == 0) {
      policy.withColumn("established", lit(false))
        .withColumn("DailyAdGroupCount", lit(0))
        .withColumn("DailyCampaignCount", lit(0))
        .withColumn("DailyAdvertiserCount", lit(0))
        .selectAs[PolicyTableWithDailyCounts]
    } else {
      val warmUpComparisonDate = date.minusDays(Config.PositivesCountWarmUpDays + 1)
      val preWarmUpPolicy = AdGroupPolicyDataset().readDate(warmUpComparisonDate)

      val adGroupPosSummary = DailyPositiveCountSummaryDataset().readRange(date.minusDays(Config.GlobalDataLookBack - 1), date, true)
        .groupBy("AdGroupId", "CampaignId", "AdvertiserId").agg((sum('Count) / Config.GlobalDataLookBack).as("DailyAdGroupCount"))

      policy.join(multiLevelJoinWithPolicy[StaticPolicyTable](policy, preWarmUpPolicy, "left_semi").select("AdGroupId").withColumn("established", lit(true)), Seq("AdGroupId"), "left")
        .withColumn("established", coalesce('established, lit(false)))
        .join(adGroupPosSummary, Seq("AdGroupId", "CampaignId", "AdvertiserId"), "left")
        .join(adGroupPosSummary.groupBy("CampaignId").agg(sum('DailyAdGroupCount).as("DailyCampaignCount")), Seq("CampaignId"), "left")
        .join(adGroupPosSummary.groupBy("AdvertiserId").agg(sum('DailyAdGroupCount).as("DailyAdvertiserCount")), Seq("AdvertiserId"), "left")
        .withColumn("DailyAdGroupCount", coalesce('DailyAdGroupCount, lit(0)))
        .withColumn("DailyCampaignCount", coalesce('DailyCampaignCount, lit(0)))
        .withColumn("DailyAdvertiserCount", coalesce('DailyAdvertiserCount, lit(0)))
        .selectAs[PolicyTableWithDailyCounts]
    }
  }

  def getActiveCPAAdGroups(date: LocalDate): Dataset[PolicyTableAdGroup] = {
    val cpaAdGroups = AdGroupDataSet().readLatestPartitionUpTo(date, true)
      .filter('ROIGoalTypeId === lit(5))
      .select("AdGroupId", "CampaignId", "AdvertiserId", "PartnerId")
    val activeCampaigns = CampaignDataSet().readLatestPartitionUpTo(date, true)
      .filter(('StartDate <= lit(date) && ('EndDate.isNull || 'EndDate > lit(date))))
      .select("CampaignId", "CustomCPATypeId")
    val activeCampaignFlights = CampaignFlightDataSet().readLatestPartitionUpTo(date, true)
      .filter('EndDateExclusiveUTC > lit(date) && 'IsDeleted === lit(false))
    val advertisers = AdvertiserDataSet().readLatestPartitionUpTo(date, true)
      .select("AdvertiserId", "AttributionClickLookbackWindowInSeconds", "AttributionImpressionLookbackWindowInSeconds")
    val spendingPartners = PartnerDataSet().readLatestPartitionUpTo(date, true)
      .filter('SpendDisabled === lit(0))
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)
      .select("CampaignId", "IncludeInCustomCPA", "ReportingColumnId")

    // Active logic from adplatform/DB/Provisioning/ProvisioningDB/Schema Objects/Views/dbo.vw_ActiveAdGroups.view.sql
    cpaAdGroups.join(activeCampaigns, Seq("CampaignId"), "inner")
      .join(activeCampaignFlights, Seq("CampaignId"), "leftsemi")
      .join(advertisers, Seq("AdvertiserId"), "inner")
      .join(spendingPartners, Seq("PartnerId"), "leftsemi")
      .join(ccrc, Seq("CampaignId"), "inner")
      .filter(('CustomCPATypeId === lit(0) && 'ReportingColumnId === lit(1)) || ('CustomCPATypeId > lit(0) && 'IncludeInCustomCPA))
      .selectAs[PolicyTableAdGroup]
      .distinct()
  }

  def generateAdGroupPolicy(date: LocalDate): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    val activeAdGroups = getActiveCPAAdGroups(date)

    val yesterdaysPolicy = Config.TryMaintainConfigValue match {
      case false => Seq[AdGroupPolicyRecord]().toDF().selectAs[AdGroupPolicyRecord]
      case _ => AdGroupPolicyDataset().readDate(date.minusDays(1))
    }

    val stage1Policy = activeAdGroups
      .withColumn("MinDailyConvCount", lit(Config.GlobalMinDailyConvCount))
      .withColumn("MaxDailyConvCount", lit(Config.GlobalMaxDailyConvCount))
      .withColumn("DataLookBack", lit(Config.GlobalDataLookBack))
      .withColumn("PositiveSamplingRate", lit(Config.GlobalPositiveSamplingRate))
      .withColumn("NegativeSamplingMethod", lit(Config.GlobalNegativeSamplingMethod))
      .withColumn("CrossDeviceConfidenceLevel", lit(Config.GlobalCrossDeviceConfidenceLevel))
      .withColumn("FetchOlderTrainingData", lit(Config.GlobalFetchOlderTrainingData))
      .withColumn("OldTrainingDataEpoch", lit(Config.GlobalOldTrainingDataEpoch))
      .selectAs[StaticPolicyTable]

    val fullPolicyTableRaw = stage1Policy
      .transform(addPositiveCountSummary) // Adds established and DailyCount
      .transform(addAggregationLevel) // Updates DataAggKey and DataAggValue
      .transform(addLastTouchCount(_, yesterdaysPolicy)) // Adds the column `LastTouchCount`
      .withColumnRenamed("AdGroupId", "ConfigValue")
      .withColumn("ConfigKey", lit("AdGroupId"))
      .selectAs[AdGroupPolicyRecord]

    removeSuperfluousAdGroups(date, fullPolicyTableRaw, yesterdaysPolicy, activeAdGroups)
  }

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("GenerateAdGroupPolicy"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val (policies, adGroupMappings) = generateAdGroupPolicy(date)

    val adGroupPolicyRows = AdGroupPolicyDataset().writePartition(policies, date, Some(1))
    val adGroupPolicyMappingRows = AdGroupPolicyMappingDataset().writePartition(adGroupMappings, date, Some(100))

    outputRowsWrittenGauge.labels("AdGroupPolicyDataset").set(adGroupPolicyRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
