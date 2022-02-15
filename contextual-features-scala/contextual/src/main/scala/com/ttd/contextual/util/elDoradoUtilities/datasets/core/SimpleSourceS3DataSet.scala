package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config

/**
 * Represents a simple non-partitioned dataset for SourceDataSet on S3.
 * As it's externally provided data it doesn't follow path template induced by El-dorado core classes,
 * hence requires static path to dataset.
 * Does not expose read/write method expecting they are implemented in the inherited class suitable to consumer needs.
 * For simple straightforward cases there is <code>SimpleSourceReadS3DataSet</code> with exposed read method.
 * @see SimpleSourceReadS3DataSet
 *
 * @param s3RootPath
 * @param rootFolderPath a relative path from the root folder to where this dataset is stored.
 * @param fileFormat
 * @param mergeSchema
 * @tparam T A case class containing the names and data types of all columns in this data set
 */
abstract class SimpleSourceS3DataSet[T <: Product : Manifest](
    s3RootPath: String, rootFolderPath: String, fileFormat: FileFormat = Parquet, mergeSchema: Boolean = false
)
  extends S3DataSet[T](SourceDataSet, s3RootPath, rootFolderPath, fileFormat, mergeSchema) {

  protected override val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s3Root))

  protected override val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s3Root))
}


