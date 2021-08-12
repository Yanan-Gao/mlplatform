package com.ttd.ml.datasets.generated.contextual.web.classification

import com.ttd.ml.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet}


case class ContentClassificationScaleRecord(
                                             Category: String,
                                             ConfidenceBuckets: Seq[Double],
                                             CumulativeDistribution: Seq[Double]
                                           )

case class ContentClassificationStickinessRecord(
                                             StickinessDomain: Double,
                                             StickinessPath1: Double,
                                             StickinessPath2: Double
                                           )

/* Classification Scale Metric */
case class IABContentClassificationScale(version: Int = 1) extends DatePartitionedS3DataSet[ContentClassificationScaleRecord](
  GeneratedDataSet,
  IABContentClassification(version).metricsRoot,
  IABContentClassification(version).metricsPath + "/scale")

/* Classification Scale Metric */
case class IABContentClassificationStickiness(version: Int = 1) extends DatePartitionedS3DataSet[ContentClassificationStickinessRecord](
  GeneratedDataSet,
  IABContentClassification(version).metricsRoot,
  IABContentClassification(version).metricsPath + "/stickiness")