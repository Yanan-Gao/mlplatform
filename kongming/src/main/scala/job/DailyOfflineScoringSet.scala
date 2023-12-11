package job

import com.thetradedesk.geronimo.shared.intModelFeaturesCols
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{AdGroupPolicyMappingDataset, DailyBidsImpressionsDataset, BidsImpressionsSchema, DailyOfflineScoringDataset}
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._

object DailyOfflineScoringSet extends KongmingBaseJob {

  override def jobName: String = "DailyOfflineScoringSet"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val mapping = AdGroupPolicyMappingDataset().readDate(date)
    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)
      .join(mapping, Seq("AdGroupId"), "left_semi")
      .selectAs[BidsImpressionsSchema]

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !(seqFields ++ directFields).contains(x))
    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(seqFields ++ directFields) ++ aliasedModelFeatureCols(keptFields)

    val scoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      selectionTabular
    )(getPrometheus)

    //assuming Yuehan has implemented the tfrecord write this way. has dependency on the changes she is doing.
    val dailyOfflineScoringRows = DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(partCount.DailyOfflineScoring))

    Array(dailyOfflineScoringRows)

  }
}
