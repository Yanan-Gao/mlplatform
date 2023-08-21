package job

import com.thetradedesk.geronimo.shared.intModelFeaturesCols
import com.thetradedesk.kongming.features.Features._
import com.thetradedesk.kongming._
import com.thetradedesk.kongming.datasets.{DailyBidsImpressionsDataset, DailyOfflineScoringDataset}
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform

object DailyOfflineScoringSet extends KongmingBaseJob {

  override def jobName: String = "DailyOfflineScoringSet"

  override def runTransform(args: Array[String]): Array[(String, Long)] = {

    val bidsImpressionFilterByPolicy = DailyBidsImpressionsDataset().readDate(date)

    var hashFeatures = modelDimensions ++ modelFeatures
    hashFeatures = hashFeatures.filter(x => !(seqFields ++ directFields).contains(x))
    val selectionTabular = intModelFeaturesCols(hashFeatures) ++ rawModelFeatureCols(seqFields ++ directFields) ++ aliasedModelFeatureCols(keptFields)

    val scoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      date,
      bidsImpressionFilterByPolicy,
      selectionTabular
    )(getPrometheus)

    //assuming Yuehan has implemented the tfrecord write this way. has dependency on the changes she is doing.
    val dailyOfflineScoringRows = DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(1000))

    Array(dailyOfflineScoringRows)

  }
}
