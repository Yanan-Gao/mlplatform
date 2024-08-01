package job

import com.thetradedesk.geronimo.shared.shiftModUdf
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupDataSet, AdGroupPolicyDataset, AdGroupPolicyMappingDataset, AdGroupPolicyMappingRecord, AdGroupPolicyRecord, AdvertiserDataSet, CampaignConversionReportingColumnDataSet, CampaignDataSet, CampaignFlightDataSet, CampaignROIGoalDataSet, DailyPositiveCountSummaryDataset, PartnerDataSet, PartnerGroupDataSet}
import com.thetradedesk.kongming.transform.TrainSetFeatureMappingTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, _}

import java.time.LocalDate

object AdGroupPolicyGenerator extends KongmingBaseJob {

  override def jobName: String = "GenerateAdGroupPolicy"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val (policies, adGroupMappings) = generateAdGroupPolicy(date)

    val adGroupPolicyRows = AdGroupPolicyDataset().writePartition(policies, date, Some(partCount.AdGroupPolicy))
    val adGroupPolicyMappingRows = AdGroupPolicyMappingDataset().writePartition(adGroupMappings, date, Some(partCount.AdGroupPolicyMapping))

    Array(adGroupPolicyRows, adGroupPolicyMappingRows)

  }

  object Config {
    var GlobalMinDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMinDailyConvCount", 1)
    var GlobalMaxDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMaxDailyConvCount", 50000)
    var GlobalLastTouchCount = config.getInt("AdGroupPolicyGenerator.GlobalLastTouchCount", 5)
    // TODO: set to 30 for roas
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

  case class SkeletalPolicyTable(CampaignId: String,
                                 AdvertiserId: String,
                                 ConfigKey: String,
                                 ConfigValue: String,
                                 AttributionClickLookbackWindowInSeconds: Int,
                                 AttributionImpressionLookbackWindowInSeconds: Int
                                )

  case class StaticPolicyTable(CampaignId: String,
                               AdvertiserId: String,
                               ConfigKey: String,
                               ConfigValue: String,
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

  case class PolicyTableWithDailyCounts(CampaignId: String,
                                        AdvertiserId: String,
                                        ConfigKey: String,
                                        ConfigValue: String,
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
                                        DailyCampaignCount: Double,
                                        DailyAdvertiserCount: Double
                                       )

  case class PolicyTableWithAggregationLevel(CampaignId: String,
                                             AdvertiserId: String,
                                             ConfigKey: String,
                                             ConfigValue: String,
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
                                             DailyCampaignCount: Double,
                                             DailyAdvertiserCount: Double,
                                             DataAggKey: String,
                                             DataAggValue: String,
                                             DailyDataAggGroupCount: Double
                                            )

  case class PolicyTableWithLastTouchCount(CampaignId: String,
                                           AdvertiserId: String,
                                           ConfigKey: String,
                                           ConfigValue: String,
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
        new CategoricalRule[String]('DataAggKey, "AdvertiserId", 'AdvertiserId)).get())
      .withColumn("DailyDataAggGroupCount", when('DataAggKey === lit("CampaignId"), 'DailyCampaignCount).when('DataAggKey === lit("AdvertiserId"), 'DailyAdvertiserCount).otherwise(0))
      .selectAs[PolicyTableWithAggregationLevel]
  }

  def addLastTouchCount(policy: Dataset[PolicyTableWithAggregationLevel], yesterdaysPolicy: Dataset[AdGroupPolicyRecord], adgroups: Dataset[PolicyTableAdGroup]): Dataset[PolicyTableWithLastTouchCount] = {
    val yesterdaysPolicyWithCampaign = yesterdaysPolicy
      .join(adgroups.select('AdGroupId.as("ConfigValue"), 'CampaignId), Seq("ConfigValue"), "inner")
      .select('CampaignId, 'DataAggKey, 'DataAggValue, lit(true).as("dataAggLevelPresentYesterday"))

    policy
      .join(yesterdaysPolicyWithCampaign, Seq("CampaignId", "DataAggKey", "DataAggValue"), "left")
      .withColumn("LastTouchCount", when('established && ('DailyDataAggGroupCount < lit(Config.LastTouchCountRareThreshold)) && 'dataAggLevelPresentYesterday.isNotNull,
          Config.LastTouchCountRareLastTouchCount).otherwise(
          Config.GlobalLastTouchCount
        ))
      .selectAs[PolicyTableWithLastTouchCount]
  }

  def addPositiveCountSummary(policy: Dataset[StaticPolicyTable]): Dataset[PolicyTableWithDailyCounts] = {
    // Add the rolling mean ad group, campaign and advertiser counts if `PositivesCountWarmUpDays` is non zero. If the config item is set to zero, disable
    // the dynamic logic
    if (Config.PositivesCountWarmUpDays == 0) {
      policy.withColumn("established", lit(false))
        .withColumn("DailyCampaignCount", lit(0))
        .withColumn("DailyAdvertiserCount", lit(0))
        .selectAs[PolicyTableWithDailyCounts]
    } else {
      val warmUpComparisonDate = date.minusDays(Config.PositivesCountWarmUpDays + 1)
      val preWarmUpPolicy = AdGroupPolicyMappingDataset().readDate(warmUpComparisonDate)

      val campaignPosSummary = DailyPositiveCountSummaryDataset().readRange(date.minusDays(Config.GlobalDataLookBack - 1), date, true)
        .groupBy("CampaignId", "AdvertiserId").agg((sum('Count) / Config.GlobalDataLookBack).as("DailyCampaignCount"))

      policy.join(preWarmUpPolicy.select('CampaignId, lit(true).as("established")).distinct(), Seq("CampaignId"), "left")
        .withColumn("established", coalesce('established, lit(false)))
        .join(campaignPosSummary, Seq("CampaignId", "AdvertiserId"), "left")
        .join(campaignPosSummary.groupBy("AdvertiserId").agg(sum('DailyCampaignCount).as("DailyAdvertiserCount")), Seq("AdvertiserId"), "left")
        .withColumn("DailyCampaignCount", coalesce('DailyCampaignCount, lit(0)))
        .withColumn("DailyAdvertiserCount", coalesce('DailyAdvertiserCount, lit(0)))
        .selectAs[PolicyTableWithDailyCounts]
    }
  }

  def getActiveAdGroups(date: LocalDate): Dataset[PolicyTableAdGroup] = {

    val customGoalTypeCol = CustomGoalTypeId.get(task).get
    val includeInCustomGoalCol = IncludeInCustomGoal.get(task).get
    val roiGoalTypeIdValue = ROIGoalTypeId.get(task).get

    val adGroups = AdGroupDataSet().readLatestPartitionUpTo(date, true)
      .select("AdGroupId", "CampaignId", "AdvertiserId", "PartnerId", "ROIGoalTypeId")
    var campaignROIGoal = CampaignROIGoalDataSet().readLatestPartitionUpTo(date, true)
    campaignROIGoal = task match {
      case "roas" => campaignROIGoal
      case _ => campaignROIGoal.filter($"Priority"===lit(1))
    }
    val activeCampaigns = CampaignDataSet().readLatestPartitionUpTo(date, true)
      .join(campaignROIGoal, Seq("CampaignId"), "left")
      .filter(('StartDate <= lit(date) && ('EndDate.isNull || 'EndDate > lit(date))))
      .filter(('ROIGoalTypeId === lit(roiGoalTypeIdValue)) || ('ROIGoalTypeId.isNull) )
      .select(col("CampaignId"), col(customGoalTypeCol), col("ROIGoalTypeId").as("CampaignROIGoalTypeId"))
    val activeCampaignFlights = CampaignFlightDataSet().readLatestPartitionUpTo(date, true)
      .filter('EndDateExclusiveUTC > lit(date) && 'IsDeleted === lit(false))
    val advertisers = AdvertiserDataSet().readLatestPartitionUpTo(date, true)
      .select("AdvertiserId", "AttributionClickLookbackWindowInSeconds", "AttributionImpressionLookbackWindowInSeconds")
    val spendingPartners = PartnerDataSet().readLatestPartitionUpTo(date, true)
      .filter('SpendDisabled === lit(0))
    val nonWalmartPartnerGroups = PartnerGroupDataSet().readLatestPartitionUpTo(date, true)
      .filter('TenantId =!= lit(2))
    val ccrc = CampaignConversionReportingColumnDataSet().readLatestPartitionUpTo(date, true)
      .select("CampaignId", includeInCustomGoalCol, "ReportingColumnId")

    // Active logic from adplatform/DB/Provisioning/ProvisioningDB/Schema Objects/Views/dbo.vw_ActiveAdGroups.view.sql
    adGroups
      .join(activeCampaigns, Seq("CampaignId"), "inner")
      .filter(('ROIGoalTypeId === lit(roiGoalTypeIdValue) && 'CampaignROIGoalTypeId.isNull) || ('CampaignROIGoalTypeId === lit(roiGoalTypeIdValue)) )
      .join(activeCampaignFlights, Seq("CampaignId"), "leftsemi")
      .join(advertisers, Seq("AdvertiserId"), "inner")
      .join(spendingPartners, Seq("PartnerId"), "inner")
      .join(nonWalmartPartnerGroups, Seq("PartnerGroupId"), "left_semi")
      .join(ccrc, Seq("CampaignId"), "inner")
      .filter((col(customGoalTypeCol) === lit(0) && 'ReportingColumnId === lit(1)) || (col(customGoalTypeCol) > lit(0) && col(includeInCustomGoalCol)))
      .selectAs[PolicyTableAdGroup]
      .distinct()
  }

  def generateSkeletalPolicyTableAndMappings(adgroups: Dataset[PolicyTableAdGroup], yesterdaysMapping: Dataset[AdGroupPolicyMappingRecord]): (Dataset[SkeletalPolicyTable], Dataset[AdGroupPolicyMappingRecord]) = {
    // Pick a random config value from yesterdays mapping table if available (there should only be one, but just in case), and pick a random ad group from todays active ad groups if it's a new campaign.
    // TODO: the rank here is required when one policy table row can collate multiple multiple campaigns. Remove after everything is stable
    val activeMappingsFromYesterday = yesterdaysMapping.select("ConfigValue").distinct
      .join(adgroups.select('AdGroupId.as("ConfigValue"), 'CampaignId), Seq("ConfigValue"), "inner")
      .withColumn("rank", rank().over(Window.partitionBy("CampaignId").orderBy("ConfigValue")))
      .filter('rank === lit(1)).drop("rank")

    val policy = adgroups
      .withColumn("rank", rank().over(Window.partitionBy("CampaignId").orderBy("AdGroupId")))
      .filter('rank === lit(1)).drop("rank")
      .join(activeMappingsFromYesterday, Seq("CampaignId"), "left")
      .withColumn("ConfigKey", lit("AdGroupId"))
      .withColumn("ConfigValue", coalesce('ConfigValue, 'AdGroupId))
      .selectAs[SkeletalPolicyTable]
      .distinct()

    val mapping = adgroups.join(policy.select("ConfigKey", "ConfigValue", "CampaignId"), Seq("CampaignId"), "inner")
      .withColumn("AdGroupIdInt", shiftModUdf(xxhash64('AdGroupId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("AdGroupId"))))
      .withColumn("CampaignIdInt", shiftModUdf(xxhash64('CampaignId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("CampaignId"))))
      .withColumn("AdvertiserIdInt", shiftModUdf(xxhash64('AdvertiserId), lit(TrainSetFeatureMappingTransform.tryGetFeatureCardinality("AdvertiserId"))))
      .selectAs[AdGroupPolicyMappingRecord]
      .distinct()

    (policy, mapping)
  }

  def getYesterdaysPolicyAndMapping(date: LocalDate): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    if (Config.TryMaintainConfigValue) {
      (AdGroupPolicyDataset().readDate(date.minusDays(1)), AdGroupPolicyMappingDataset().readDate(date.minusDays(1)))
    } else {
      (Seq[AdGroupPolicyRecord]().toDF().selectAs[AdGroupPolicyRecord], Seq[AdGroupPolicyMappingRecord]().toDF().selectAs[AdGroupPolicyMappingRecord])
    }
  }

  def addStaticFields(skeletalPolicyTable: Dataset[SkeletalPolicyTable]): Dataset[StaticPolicyTable] = {
    skeletalPolicyTable
      .withColumn("MinDailyConvCount", lit(Config.GlobalMinDailyConvCount))
      .withColumn("MaxDailyConvCount", lit(Config.GlobalMaxDailyConvCount))
      .withColumn("DataLookBack", lit(Config.GlobalDataLookBack))
      .withColumn("PositiveSamplingRate", lit(Config.GlobalPositiveSamplingRate))
      .withColumn("NegativeSamplingMethod", lit(Config.GlobalNegativeSamplingMethod))
      .withColumn("CrossDeviceConfidenceLevel", lit(Config.GlobalCrossDeviceConfidenceLevel))
      .withColumn("FetchOlderTrainingData", lit(Config.GlobalFetchOlderTrainingData))
      .withColumn("OldTrainingDataEpoch", lit(Config.GlobalOldTrainingDataEpoch))
      .selectAs[StaticPolicyTable]
  }

  def generateAdGroupPolicy(date: LocalDate): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    val activeAdGroups = getActiveAdGroups(date)

    val (yesterdaysPolicy, yesterdaysMapping) = getYesterdaysPolicyAndMapping(date)

    val (skeletalPolicyTable, mappings) = generateSkeletalPolicyTableAndMappings(activeAdGroups, yesterdaysMapping)

    val policyTable = skeletalPolicyTable
      .transform(addStaticFields)
      .transform(addPositiveCountSummary) // Adds established and DailyCount
      .transform(addAggregationLevel) // Updates DataAggKey and DataAggValue
      .transform(addLastTouchCount(_, yesterdaysPolicy, activeAdGroups)) // Adds the column `LastTouchCount`
      .selectAs[AdGroupPolicyRecord]

    (policyTable, mappings)
  }

}
