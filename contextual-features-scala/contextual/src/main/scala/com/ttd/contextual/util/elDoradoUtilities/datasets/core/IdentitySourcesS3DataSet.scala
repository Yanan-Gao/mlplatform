package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config

abstract class IdentitySourcesS3DataSet[T <: Product](rootFolderPath: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
  extends DatePartitionedS3DataSet[T](SourceDataSet, S3Roots.IDENTITY_SOURCES_ROOT, rootFolderPath, mergeSchema = mergeSchema, dateTimeFormatString = DefaultTimeFormatStrings.dateTimeFormatStringWithDashes) {
  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}


abstract class IdentitySourcesHourlyDataSet[T <: Product](rootFolderPath: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
  extends DateHourPartitionedS3DataSet[T](
    SourceDataSet,
    S3Roots.IDENTITY_SOURCES_ROOT,
    rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "hour" -> ColumnExistsInDataSet,
    Parquet,
    mergeSchema,
    DateHourFormatStrings(
      DefaultTimeFormatStrings.dateTimeFormatStringWithDashes,
      DefaultTimeFormatStrings.hourTimeFormatString
    )
  ) {
  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}