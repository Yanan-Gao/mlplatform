package job

import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, STRING_FEATURE_TYPE, intModelFeaturesCols, loadParquetData}
import com.thetradedesk.kongming.features.Features.{aliasedModelFeatureCols, keptFields, modelDimensions, modelFeatures, rawModelFeatureCols, seqFields}
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{DailyBidsImpressionsDataset, DailyOfflineScoringDataset}
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.spark.util.TTDConfig.config

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object DailyOfflineScoringSet {

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient(KongmingApplicationName, getJobNameWithExperimentName("DailyOfflineScoringSet"))
    val jobDurationGauge = prometheus.createGauge(RunTimeGaugeName, "Job execution time in seconds")
    val jobDurationGaugeTimer = jobDurationGauge.startTimer()
    val outputRowsWrittenGauge = prometheus.createGauge(OutputRowCountGaugeName, "Number of rows written", "DataSet")

    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !seqFields.contains(x))
    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(seqFields) ++ aliasedModelFeatureCols(keptFields)

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
