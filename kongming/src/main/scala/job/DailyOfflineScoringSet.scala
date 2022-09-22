package job

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, STRING_FEATURE_TYPE, loadParquetData}
import com.thetradedesk.kongming._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.intModelFeaturesCols
import com.thetradedesk.kongming.datasets.{AdGroupPolicyDataset, DailyOfflineScoringDataset}
import com.thetradedesk.kongming.transform.OfflineScoringSetTransform
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column
import job.GenerateTrainSet.{modelDimensions, modelFeatures}

import java.time.LocalDate

object DailyOfflineScoringSet {

  val keptFields: Array[ModelFeature] = Array(
    ModelFeature("BidRequestId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, None, 0),
  )

  def modelKeepFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name).alias(f.name+"Str")).toArray
  }

  def modelKeepFeatureColNames(features: Seq[ModelFeature]): Array[String] = {
    features.map(f => f.name+"Str").toArray
  }

  def main(args: Array[String]): Unit = {

    val prometheus = new PrometheusClient("KoaV4Conversion", "DailyOfflineScoringSet")

    val bidsImpressions = loadParquetData[BidsImpressionsSchema](BidsImpressionsS3Path, date, source = Some(GERONIMO_DATA_SOURCE))

    val adGroupPolicyHardCodedDate = policyDate
    val adGroupPolicy = AdGroupPolicyDataset.readHardCodedDataset(adGroupPolicyHardCodedDate)

    val selectionTabular = intModelFeaturesCols(modelDimensions ++ modelFeatures) ++ modelKeepFeatureCols(keptFields)

    val scoringFeatureDS = OfflineScoringSetTransform.dailyTransform(
      bidsImpressions,
      adGroupPolicy,
      selectionTabular
    )(prometheus)

    //assuming Yuehan has implemented the tfrecord write this way. has dependency on the changes she is doing.
    DailyOfflineScoringDataset().writePartition(scoringFeatureDS, date, Some(100))

  }
}