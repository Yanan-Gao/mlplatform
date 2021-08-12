package com.ttd.ml.datasets.generated.contextual.web.classification

import com.ttd.ml.datasets.S3Paths
import com.ttd.ml.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet, S3DataSet}

case class ContentClassificationRecord(
                                        Url: String,
                                        Categories: Seq[Categories]
//                                        Categories: Seq[String],
//                                        Confidences: Seq[Double]
                                      )

/* Content embeddings */
case class IABContentClassification(version: Int = 1) extends DatePartitionedS3DataSet[ContentClassificationRecord](
  GeneratedDataSet,
  S3Paths.FEATURE_STORE_ROOT,
  S3Paths.versionedPath(S3Paths.WEB_CLASSIFICATION_PATH, WebClassificationFIDs.IABContentClassification.fid, version)) {
  val metricsRoot: String = s3Root + S3Paths.METRICS
  val metricsPath: String = rootFolderPath
}

case class Categories(Tag: String, DealIdIndexes: Seq[Int])
