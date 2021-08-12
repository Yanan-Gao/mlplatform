package com.ttd.ml.util.elDoradoUtilities.datasets.core

import org.apache.spark.sql.Dataset

/**
  * Represents a simple non-partitioned dataset for SourceDataSet on S3.
  * As it's externally provided data it doesn't follow path template induced by El-dorado core classes,
  * hence requires static path to dataset.
  * Has <code>read</code> method exposed.
  *
  * @param s3RootPath
  * @param rootFolderPath a relative path from the root folder to where this dataset is stored.
  * @param fileFormat
  * @param mergeSchema
  * @tparam T A case class containing the names and data types of all columns in this data set
  */
abstract class SimpleSourceReadS3DataSet[T <: Product : Manifest](
    s3RootPath: String, rootFolderPath: String, fileFormat: FileFormat = Parquet, mergeSchema: Boolean = false
)
  extends SimpleSourceS3DataSet[T](s3RootPath, rootFolderPath, fileFormat, mergeSchema) {

  /** Read from this dataset. This reads from different locations based on the value of the ttd.env flag: prod: read from production on s3.
    * sandbox: If this is a Source dataset, read from production. If this is a Generated dataset, read from the users sandbox directory on s3
    * local: Read from the local-data folder in this project
    *
    * @param rootFolderPaths Optional parameter with arbitrary list of path to read from.
    *                        If this parameter is empty the path provided in the constructor param `rootFolderPath` will be used
    *                        (default behavior).
    * @return A DataSet[T]
    */
  override def read(rootFolderPaths: String*): Dataset[T] = super.read(rootFolderPaths: _*)
}
