package com.ttd.ml.util.elDoradoUtilities.datasets.core

import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config

abstract class ProvisioningS3DataSet[T <: Product](rootFolderPath: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
  extends DatePartitionedS3DataSet[T](SourceDataSet, S3Roots.PROVISIONING_ROOT, rootFolderPath, mergeSchema = mergeSchema) {
  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}



abstract class ProvisioningS3HourlyDataSet[T <: Product](rootFolderPath: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
  extends DateHourPartitionedS3DataSet[T](
    SourceDataSet,
    S3Roots.PROVISIONING_ROOT,
    rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "hour" -> ColumnExistsInDataSet,
    Parquet,
    mergeSchema
  ) {
  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}