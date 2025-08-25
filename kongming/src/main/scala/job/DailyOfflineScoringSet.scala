package job

import com.thetradedesk.geronimo.shared.intModelFeaturesCols
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets._
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.functions.{broadcast, col}


object DailyOfflineScoringSet extends KongmingBaseJob {

  override def jobName: String = "DailyOfflineScoringSet"

  val saveTrainingDataAsTFRecord = config.getBoolean("saveTrainingDataAsTFRecord", true)
  val saveTrainingDataAsCSV = config.getBoolean("saveTrainingDataAsCSV", true)
  val saveTrainingDataAsCBuffer = config.getBoolean("saveTrainingDataAsCBuffer", true)
  val saveTrainingDataAsParquet = config.getBoolean("saveTrainingDataAsParquet", true)
  val partitionCount = config.getInt("partitionCount", partCount.DailyOfflineScoring)

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val mapping = AdGroupPolicyMappingDataset().readDate(date)
    val bidsImpressionFilterByPolicy = DailyHourlyBidsImpressionsDataset().readDate(date)
      .join(broadcast(mapping), Seq("AdGroupId"), "left_semi")
      .selectAs[BidsImpressionsSchema]

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !(seqDirectFields ++ directFields ++ flagFields).contains(x))

    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(seqDirectFields) ++ aliasedModelFeatureCols(keptFields ++ directFields)

    //val dailyOfflineScoringRows = if (task == "roas") {
    val oldScoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      selectionTabular
    )(getPrometheus)

    var dailyOfflineScoringRows = ("", 0L)

    if (saveTrainingDataAsTFRecord) {
      dailyOfflineScoringRows = OldDailyOfflineScoringDataset().writePartition(oldScoringFeatureDS.selectAs[OldDailyOfflineScoringRecord], date, Some(partitionCount))
    }

    if (saveTrainingDataAsParquet) {
      dailyOfflineScoringRows = OldDailyOfflineScoringParquetDataset().writePartition(oldScoringFeatureDS.selectAs[OldDailyOfflineScoringRecord], date, Some(partitionCount))
    }

    if (saveTrainingDataAsCSV) {
      val reselectionTabular = oldScoringFeatureDS.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqHashFields ++ seqDirectFields)
      val scoringFeatureDS = oldScoringFeatureDS
        .select(reselectionTabular: _*)
        .selectAs[DailyOfflineScoringRecord]

      dailyOfflineScoringRows = DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(partitionCount))
    }

    if (saveTrainingDataAsCBuffer) {
      dailyOfflineScoringRows = ArrayDailyOfflineScoringDataset().writePartition(
        encodeDatasetForCBuffer[ArrayDailyOfflineScoringRecord](oldScoringFeatureDS),
        date, None, partitionCount, evalBatchSize)
    }

    Array(dailyOfflineScoringRows)

  }
}
