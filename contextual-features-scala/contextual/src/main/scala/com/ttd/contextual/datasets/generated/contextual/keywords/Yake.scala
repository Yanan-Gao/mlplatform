package com.ttd.contextual.datasets.generated.contextual.keywords

import com.ttd.contextual.datasets.S3Paths
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.GeneratedDataSet
import com.ttd.contextual.datasets.S3Paths
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet}

case class Yake(version: Int = 1) extends DatePartitionedS3DataSet[YakeRecord](
  GeneratedDataSet,
  S3Paths.FEATURE_STORE_ROOT,
  S3Paths.versionedPath(S3Paths.WEB_KEYWORDS_PATH, KeywordFIDs.YakeFID.fid, version))

case class YakeRecord(
                       Url: String,
                       Keyphrases: Seq[(String, Double)])