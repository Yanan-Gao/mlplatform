package job

import com.thetradedesk.geronimo.shared.intModelFeaturesCols
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingDataset, DailyBidsImpressionsDataset, BidsImpressionsSchema, DailyOfflineScoringDataset, DailyOfflineScoringRecord, OldDailyOfflineScoringDataset}
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.functions.col

object DailyOfflineScoringSet extends KongmingBaseJob {

  override def jobName: String = "DailyOfflineScoringSet"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {
    val mapping = AdGroupPolicyMappingDataset().readDate(date)
    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)
      .join(mapping, Seq("AdGroupId"), "left_semi")
      .selectAs[BidsImpressionsSchema]

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !(seqFields ++ directFields).contains(x))
    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(seqFields) ++ aliasedModelFeatureCols(keptFields ++ directFields)

    //val dailyOfflineScoringRows = if (task == "roas") {
    val scoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      selectionTabular
    )(getPrometheus)

    val dailyOfflineScoringRows = OldDailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(partCount.DailyOfflineScoring))
    //} else {
      //val oldScoringFeatureDS = OfflineScoringSetTransform.dailyTransform(date, bidsImpression, selectionTabular)(getPrometheus)
      //val reselectionTabular = oldScoringFeatureDS.columns.map { c => col(c) }.toArray ++ aliasedModelFeatureCols(seqFields)
      //val scoringFeatureDS = oldScoringFeatureDS
        //.select(reselectionTabular: _*)
        //.selectAs[DailyOfflineScoringRecord]

      //DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(partCount.DailyOfflineScoring))
    //}

    Array(dailyOfflineScoringRows)

  }
}
