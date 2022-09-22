package job

import com.thetradedesk.kongming.datasets.{AdGroupCvrForBiasTuningDataset, AdGroupPolicyDataset, ImpressionForIsotonicRegDataset, UnifiedAdGroupDataSet}
import com.thetradedesk.kongming.{date, policyDate}
import com.thetradedesk.kongming.transform.TrainSetTransformation.getWeightsForTrackingTags
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.functions.{broadcast, col, lit, to_timestamp}
import com.thetradedesk.kongming.transform.OfflineAttributionTransform._

import java.time.LocalDate



object OfflineScoringSetAttribution{
  val IsotonicRegPositiveLabelCountThreshold = config.getInt(path="positiveLabelCountThresholdForIsotonicReg", 20)
  val IsotonicRegNegSampleRate = config.getDouble(path="IsotonicRegNegSampleRate", 1.0/3)


  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient("KoaV4Conversion", "OfflineScoringSetAttribution")

    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate).cache()

    // prerequisite:
    // 1. the model used to score impressions should be the same as model in production, which has modelDate
    // modelDate can be earlier than date.
    //  2. before running this job, score set of T-7 and T-6 should be generated.
    val modelDate = config.getDate("modelDate" , LocalDate.now())

    val offlineScoreSetLookbackFromModelDate = config.getInt(path="offlineScoresetLookbackFromModelDate", 6)
    val offlineScoreDays = config.getInt(path="offlineScoreDays", 5)

    // 1. load offline scores
    val offlineScoreEndDate =  date.minusDays(offlineScoreSetLookbackFromModelDate)
    val offlineScore = getOfflineScore(modelDate =modelDate, endDate = offlineScoreEndDate, lookBack =offlineScoreDays-1)(prometheus).cache()
    val totalImpressionsCount = offlineScore.count().toInt
    // get a count here for later usage

    // 2. load attributedEventDataset and attributedEventResult
    // AttributedEventTypeId: 1->Click, 2-> Impression
    // AttributionMethodId: 0 -> 'LastClick' , 1 -> 'ViewThrough' , 2 -> 'Touch'
    val attributes = getAttributedEventAndResult(endDate = date, lookBack = offlineScoreSetLookbackFromModelDate+offlineScoreDays-1)(prometheus)
    val attributedEvent =attributes._1
      .filter($"AttributedEventTypeId".isin(List("1", "2"): _*))
      .as("t1")
      .join(broadcast(adGroupPolicy).as("t2"), col("t1.AdGroupId")===col("t2.ConfigValue"))
      .withColumn("AttributedEventLogEntryTime", to_timestamp(col("AttributedEventLogEntryTime")).as("AttributedEventLogEntryTime"))

    val attributedEventResult = attributes._2
      .filter($"AttributionMethodId".isin(List("0", "1"): _*))
      .toDF()

    // load pixel weight
    val adGroupDS = UnifiedAdGroupDataSet().readLatestPartition()
    val pixelWeight = getWeightsForTrackingTags(adGroupPolicy, adGroupDS)

    // 3. attributed impressions that are in scored impressions, find the latest per conversion event
    val offlineScoreLabel = getOfflineScoreLabelWeight(offlineScore, attributedEvent, attributedEventResult, pixelWeight)(prometheus)

    // 4. get conversions, impressions, conversion rate per piece, and weight per impression for isotonic regression
    val impressionLevelPerformance =  getOfflineScoreImpressionAndPiecePerformance(offlineScoreLabel)(prometheus).cache()

    // 5. get inputs for isotonic regression and bias tuning
    val inputForCalibration = getInputForCalibrationAndBiasTuning(impressionLevelPerformance, totalImpressionsCount, adGroupPolicy)(prometheus)

    ImpressionForIsotonicRegDataset().writePartition(inputForCalibration._1, date, Some(1))
    AdGroupCvrForBiasTuningDataset().writePartition(inputForCalibration._2, date, Some(1))
  }



}

