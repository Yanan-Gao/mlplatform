package job

import com.thetradedesk.kongming.datasets.{AdGroupCvrForBiasTuningDataset, AdGroupPolicyDataset, AdGroupPolicyMappingDataset, ImpressionForIsotonicRegDataset, UnifiedAdGroupDataSet}
import com.thetradedesk.kongming.{KongmingApplicationName, OutputRowCountGaugeName, RunTimeGaugeName, date, policyDate, samplingSeed}
import com.thetradedesk.kongming.transform.TrainSetTransformation.{TrackingTagWeightsRecord, getWeightsForTrackingTags}
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions.{broadcast, col, lit, to_timestamp}
import com.thetradedesk.kongming.transform.OfflineAttributionTransform._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions

import java.time.LocalDate


object OfflineScoringSetAttribution{

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, "OfflineScoringSetAttribution")
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val IsotonicRegPositiveLabelCountThreshold = config.getInt(path="positiveLabelCountThresholdForIsotonicReg", 10)
    val IsotonicRegNegCap = config.getInt(path="IsotonicRegNegCap", 1000000)
    val IsotonicRegNegMaxSampleRate = config.getDouble(path="IsotonicRegNegMaxSampleRate", 1.0/3)

    val defaultCvr = config.getDouble(path="DefaultConversionRate", 1e-3)

    val adGroupPolicy = AdGroupPolicyDataset().readDate(date).cache()
    val adgroupBaseAssociateMapping = AdGroupPolicyMappingDataset().readDate(date)
    // prerequisite:
    // 1. the model used to score impressions should be the same as model in production, which has modelDate
    // modelDate can be earlier than date.
    //  2. before running this job, score set of T-7 and T-6 should be generated.
    val modelDate = config.getDate("modelDate" , LocalDate.now())

    val offlineScoreSetLookbackFromModelDate = config.getInt(path="offlineScoresetLookbackFromModelDate", 2)
    val offlineScoreDays = config.getInt(path="offlineScoreDays", 13)

    // 1. load offline scores
    val offlineScoreEndDate =  date.minusDays(offlineScoreSetLookbackFromModelDate)
    val offlineScore = getOfflineScore(modelDate =modelDate, endDate = offlineScoreEndDate, lookBack =offlineScoreDays-1, adgroupBaseAssociateMapping)(prometheus).cache()
    // get a count here for later usage

    // 2. load attributedEventDataset and attributedEventResult
    val attributes = getAttributedEventAndResult(adGroupPolicy = adGroupPolicy, endDate = date, lookBack = offlineScoreSetLookbackFromModelDate+offlineScoreDays-1)(prometheus)

    // load pixel weight of adgroup and extend the pixel table to campaigns
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartitionUpTo(date, true)

    val pixelWeight = getWeightsForTrackingTags(date, adGroupPolicy, adGroupDS)
    val pixelWeightForBaseAssociateAdGroup = pixelWeight.join(adgroupBaseAssociateMapping, Seq("ConfigValue","ConfigKey"))
      .withColumn("ConfigValue", $"AdGroupId").selectAs[TrackingTagWeightsRecord]

    // 3. attributed impressions that are in scored impressions, find the latest per conversion event
    val offlineScoreLabel = getOfflineScoreLabelWeight(offlineScore, attributes._1, attributes._2, pixelWeightForBaseAssociateAdGroup)(prometheus)

    // 4. get conversions, impressions, conversion rate per piece, and weight per impression for isotonic regression
    val impressionLevelPerformance =  getOfflineScoreImpressionAndPiecePerformance(offlineScoreLabel)(prometheus)

    // 5. get inputs for isotonic regression and bias tuning
    val inputForCalibration = getInputForCalibrationAndBiasTuning(impressionLevelPerformance, defaultCvr, adGroupPolicy, IsotonicRegPositiveLabelCountThreshold, IsotonicRegNegCap, IsotonicRegNegMaxSampleRate, samplingSeed)(prometheus)

    val isotonicRegRows = ImpressionForIsotonicRegDataset().writePartition(inputForCalibration._1, date, Some(1000))
    val biasTuningRows = AdGroupCvrForBiasTuningDataset().writePartition(inputForCalibration._2, date, Some(1))

    outputRowsWrittenGauge.labels("ImpressionForIsotonicRegDataset").set(isotonicRegRows)
    outputRowsWrittenGauge.labels("AdGroupCvrForBiasTuningDataset").set(biasTuningRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }



}

