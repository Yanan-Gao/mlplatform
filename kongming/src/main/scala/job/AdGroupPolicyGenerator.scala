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

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate

object AdGroupPolicyGenerator {
  object Config {
    var GlobalDataAggKey = config.getString("AdGroupPolicyGenerator.GlobalDataAggKey", "CampaignId")
    var GlobalMinDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMinDailyConvCount", 1)
    var GlobalMaxDailyConvCount = config.getInt("AdGroupPolicyGenerator.GlobalMaxDailyConvCount", 50000)
    var GlobalLastTouchCount = config.getInt("AdGroupPolicyGenerator.GlobalLastTouchCount", 5)
    var GlobalDataLookBack = config.getInt("AdGroupPolicyGenerator.GlobalDataLookBack", 15)
    var GlobalPositiveSamplingRate = config.getDouble("AdGroupPolicyGenerator.GlobalPositiveSamplingRate", 1)
    var GlobalNegativeSamplingMethod = config.getString("AdGroupPolicyGenerator.GlobalNegativeSamplingMethod", "Constant")
    var GlobalCrossDeviceConfidenceLevel = config.getDouble("AdGroupPolicyGenerator.GlobalCrossDeviceConfidenceLevel", 0.01)
    var GlobalFetchOlderTrainingData = config.getBoolean("AdGroupPolicyGenerator.GlobalFetchOlderTrainingData", false)
    var GlobalOldTrainingDataEpoch = config.getInt("AdGroupPolicyGenerator.GlobalOldTrainingDataEpoch", 2)
    var LastTouchCountWarmUpDays = config.getInt("AdGroupPolicyGenerator.LastTouchCountWarmUpDays", 7)
    var LastTouchCountRareThreshold = config.getInt("LastTouchCountRareThreshold", 200)
    var LastTouchCountRareLastTouchCount = config.getInt("LastTouchCountRareLastTouchCount", 10)
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

  def addLastTouchCount(date: LocalDate, policy: DataFrame): DataFrame = {
    val warmUpComparisonDate = date.minusDays(Config.LastTouchCountWarmUpDays + 1)
    val refAdGroupsConsidered = AdGroupPolicyDataset().readDate(warmUpComparisonDate)
      .select("DataAggKey", "DataAggValue", "DataLookBack")
      .withColumn("established", lit(true))

    val readPosCountSummary = (daysAgo: Int) => DailyPositiveCountSummaryDataset().readDate(date.minusDays(daysAgo)).withColumn("DaysAgo", lit(daysAgo))
    val daysToLoad = 1 to Config.GlobalDataLookBack
    var positiveCounts = daysToLoad.tail.foldLeft(readPosCountSummary(daysToLoad.head))((dataSoFar, nextDaysAgo) => dataSoFar.unionByName(readPosCountSummary(nextDaysAgo)))
      .join(policy.select("DataAggKey", "DataAggValue", "DataLookBack"), Seq("DataAggKey", "DataAggValue"), "inner")
      .filter('DaysAgo <= 'DataLookBack)
      .groupBy('DataAggKey, 'DataAggValue, 'DataLookBack).agg((sum('Count) / 'DataLookBack).as("Count"))

    val joined = policy
      .join(refAdGroupsConsidered, Seq("DataAggKey", "DataAggValue", "DataLookBack"), "left")
      .join(positiveCounts, Seq("DataAggKey", "DataAggValue", "DataLookBack"), "left")

    joined.withColumn("LastTouchCount", when('established && ('count / 'DataLookBack) < lit(Config.LastTouchCountRareThreshold),
      Config.LastTouchCountRareLastTouchCount).otherwise(
      Config.GlobalLastTouchCount
    )).drop("count", "established")
  }

  def removeSuperfluousAdGroups(date: LocalDate, policy: Dataset[AdGroupPolicyRecord], adgroups: DataFrame): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
    val yesterdaysPolicy = AdGroupPolicyDataset().readDate(date.minusDays(1))
      .select("ConfigKey", "ConfigValue", "DataAggKey", "DataAggValue")
      .withColumnRenamed("ConfigKey", "PolicyConfigKey")
      .withColumnRenamed("ConfigValue", "PolicyConfigValue")

    val decisions = policy.join(yesterdaysPolicy, Seq("DataAggKey", "DataAggValue"), "left")
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

  def generateAdGroupPolicy(date: LocalDate): (Dataset[AdGroupPolicyRecord], Dataset[AdGroupPolicyMappingRecord]) = {
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
      .withColumn("CrossDeviceUsage", 'CrossDeviceAttributionModelId.isNotNull)
      .select("CampaignId", "CrossDeviceUsage", "IncludeInCustomCPA", "ReportingColumnId")

    // Active logic from adplatform/DB/Provisioning/ProvisioningDB/Schema Objects/Views/dbo.vw_ActiveAdGroups.view.sql
    val activeAdGroups = cpaAdGroups.join(activeCampaigns, Seq("CampaignId"), "inner")
      .join(activeCampaignFlights, Seq("CampaignId"), "leftsemi")
      .join(advertisers, Seq("AdvertiserId"), "inner")
      .join(spendingPartners, Seq("PartnerId"), "leftsemi")
      .join(ccrc, Seq("CampaignId"), "inner")
      .filter(('CustomCPATypeId === lit(0) && 'ReportingColumnId === lit(1)) || ('CustomCPATypeId > lit(0) && 'IncludeInCustomCPA))
      .groupBy("AdGroupId", "CampaignId", "AdvertiserId", "AttributionClickLookbackWindowInSeconds", "AttributionImpressionLookbackWindowInSeconds").agg(max('CrossDeviceUsage.cast("Int")).cast("Boolean").as("CrossDeviceUsage"))

    val fullPolicyTableRaw = activeAdGroups.withColumn("DataAggKey", lit("CampaignId"))
      .withColumn("DataAggValue", new ConditionalSetting(
        new CategoricalRule[String]('DataAggKey, "CampaignId", 'CampaignId),
        new CategoricalRule[String]('DataAggKey, "AdGroupId", 'AdGroupId),
        new CategoricalRule[String]('DataAggKey, "AdvertiserId", 'AdvertiserId)).get())
      .withColumn("MinDailyConvCount", lit(Config.GlobalMinDailyConvCount))
      .withColumn("MaxDailyConvCount", lit(Config.GlobalMaxDailyConvCount))
      .withColumn("DataLookBack", lit(Config.GlobalDataLookBack))
      .withColumn("PositiveSamplingRate", lit(Config.GlobalPositiveSamplingRate))
      .withColumn("NegativeSamplingMethod", lit(Config.GlobalNegativeSamplingMethod))
      .withColumn("CrossDeviceConfidenceLevel", lit(Config.GlobalCrossDeviceConfidenceLevel))
      .withColumn("FetchOlderTrainingData", lit(Config.GlobalFetchOlderTrainingData))
      .withColumn("OldTrainingDataEpoch", lit(Config.GlobalOldTrainingDataEpoch))
      .withColumnRenamed("AdGroupId", "ConfigValue")
      .withColumn("ConfigKey", lit("AdGroupId"))
      .transform(addLastTouchCount(date, _)) // Adds the column `LastTouchCount`
      .selectAs[AdGroupPolicyRecord]

    removeSuperfluousAdGroups(date, fullPolicyTableRaw, activeAdGroups)
  }

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, "GenerateAdGroupPolicy")
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
