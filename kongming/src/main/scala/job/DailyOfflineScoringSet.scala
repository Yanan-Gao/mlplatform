package job

import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, STRING_FEATURE_TYPE, intModelFeaturesCols, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{DailyBidsImpressionsDataset, DailyOfflineScoringDataset}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import job.GenerateTrainSet.{modelDimensions, modelFeatures, seqFields}

object DailyOfflineScoringSet {

  val BidRequestIdModelFeature = ModelFeature("BidRequestId", STRING_FEATURE_TYPE, None, 0)
  val NonBidRequestIdModelFeatures = Array(
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("CampaignId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, None, 0)
  )
  val keptFields = Array(BidRequestIdModelFeature) ++ NonBidRequestIdModelFeatures

  def modelKeepFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name).alias(modelKeepFeatureColAlias(f))).toArray
  }
  def modelKeepFeatureColAlias(f: ModelFeature): String = {
    f.name + "Str"
  }

  def modelKeepFeatureColNames(features: Seq[ModelFeature]): Array[String] = {
    features.map(f => f.name+"Str").toArray
  }

  def intactFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name)).toArray
  }

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("DailyOfflineScoringSet"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !seqFields.contains(x))
    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ intactFeatureCols(seqFields) ++ modelKeepFeatureCols(keptFields)

    val scoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      selectionTabular
    )(prometheus)

    //assuming Yuehan has implemented the tfrecord write this way. has dependency on the changes she is doing.
    val dailyOfflineScoringRows = DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(1000))

    outputRowsWrittenGauge.labels("DailyOfflineScoringDataset").set(dailyOfflineScoringRows)
    jobDurationGaugeTimer.setDuration()
    prometheus.pushMetrics()

    spark.stop()
  }
}
