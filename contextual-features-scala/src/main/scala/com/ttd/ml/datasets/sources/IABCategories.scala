package com.ttd.ml.datasets.sources

import com.ttd.ml.datasets.S3Paths
import com.ttd.ml.util.elDoradoUtilities.datasets.core.{DatePartitionedS3DataSet, GeneratedDataSet}

case class IABCategories(version: Int = 1)
  extends DatePartitionedS3DataSet[IABCategoriesRecord](
    GeneratedDataSet,
    S3Paths.FEATURE_STORE_ROOT,
    s"/taxonomies/iab/",
    // this needs to be disabled for large tables, the performance hit is too big
    mergeSchema = false
  ) {
  override protected val s3RootProd: String = normalizeUri(s"$s3Root")
  override protected val s3RootTest: String = normalizeUri(s"$s3Root")
}

case class IABCategoriesRecord(Id: Int,
                          Parent: Int,
                          Name: String,
                          Path1: String,
                          Path2: String,
                          Path3: String)