package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import org.apache.spark.sql.Dataset

/**
 * Represents a simple non-partitioned dataset on S3
 *
 * @param dataSetType    Whether this is a Source dataset or a Generated Dataset. Source datasets are always drawn from prod sources,
 *                       and are usually not written to.
 *                       Generated Datasets are given special treatment in sandbox mode, where they are written to and read from in the
 *                       users personal sandbox, and are typically written in a human-readable format for easy debugging.
 * @param s3RootPath
 * @param rootFolderPath a relative path from the root folder to where this dataset is stored.
 * @param fileFormat
 * @param mergeSchema
 * @tparam T A case class containing the names and data types of all columns in this data set
 */
abstract class SimpleS3DataSet[T <: Product : Manifest](
    dataSetType: DataSetType, s3RootPath: String, rootFolderPath: String, fileFormat: FileFormat = Parquet, mergeSchema: Boolean = false
)
  extends S3DataSet[T](dataSetType, s3RootPath, rootFolderPath, fileFormat, mergeSchema) {

  override def write(
      dataSet: Dataset[T],
      coalesceToNumFiles: Option[Int] = None,
      overrideSourceWrite: Boolean = false,
      tempDirectory: String
  ): Long =
    super.write(dataSet, coalesceToNumFiles, overrideSourceWrite, tempDirectory)

  override def read(rootFolderPaths: String*): Dataset[T] = super.read(rootFolderPaths: _*)

  def read: Dataset[T] = super.read()
}
