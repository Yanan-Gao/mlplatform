package com.ttd.ml.util.elDoradoUtilities.datasets.core

import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config

abstract class DataPipeS3DataSet[T <: Product](rootFolderPath: String, timestampFieldName: String, mergeSchema: Boolean = false)(implicit m: Manifest[T])
  extends DateHourPartitionedS3DataSet[T](SourceDataSet, S3Roots.DATAPIPELINE_SOURCES, rootFolderPath, timestampFieldName, Parquet, mergeSchema,
    DateHourFormatStrings(
      DefaultTimeFormatStrings.dateTimeFormatString,
      DefaultTimeFormatStrings.hourTimeFormatString)) {
  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"$s3Root"))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root"))
}