package com.ttd.ml.util.elDoradoUtilities.datasets.core

import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config

/**
  * A trait to add to any S3DataSet subclass to override the rules for determining prod/test locations.
  * The convention for these data sets is to store data in different schemas according to the "schema.db" part of the s3 path
  * Any class that inherits from an S3DataSet subclass, and includes this trait will have the overridden values defined here
  *
  * @tparam T A case class containing the names and data types of all columns in this data set
  */
trait QuboleS3DataSetOverride[T <: Product] extends S3DataSet[T] {
  val defaultProdSchema: String = "thetradedesk"
  val defaultTestSchema: String = "thetradedesk_t"

  val testSchema: String = config.getString("quboleTestSchema", defaultTestSchema)

  private def getDefaultS3RootTest: String = {
    s3Root.replace(s"$defaultProdSchema.db", s"$testSchema.db")
  }

  override protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s3Root))
  override protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", getDefaultS3RootTest))
}

// orc files are schema-less, so spark will assume the columns are in the same order as the specified schema
// the schema is generated from the case class of the data set
abstract class DatePartitionedQuboleS3DataSet[T <: Product](dataSetType: DataSetType,
                                                            s3RootPath: String,
                                                            rootFolderPath: String,
                                                            partitionField: String,
                                                            fileFormat: FileFormat,
                                                            mergeSchema: Boolean,
                                                            dateTimeFormatString: String)(implicit m: Manifest[T])
  extends DatePartitionedS3DataSet[T](
    dataSetType,
    s3RootPath,
    rootFolderPath,
    partitionField,
    fileFormat,
    mergeSchema,
    dateTimeFormatString)
  with QuboleS3DataSetOverride[T] {

}

abstract class TtdDatePartitionedQuboleS3DataSet[T <: Product](dataSetType: DataSetType, rootFolderPath: String, partitionField: String = "date", mergeSchema: Boolean = false,
                                                               dateTimeFormatString: String = DefaultTimeFormatStrings.dateTimeFormatString,
                                                               fileFormat: FileFormat = ORC)(implicit m: Manifest[T])
  extends DatePartitionedQuboleS3DataSet[T](dataSetType, S3Roots.QUBOLE_TTD_ROOT, rootFolderPath, partitionField, fileFormat, mergeSchema, dateTimeFormatString) {
}

abstract class AdhocDatePartitionedQuboleS3DataSet[T <: Product](dataSetType: DataSetType, rootFolderPath: String, partitionField: String = "date", mergeSchema: Boolean = false,
                                                               dateTimeFormatString: String = DefaultTimeFormatStrings.dateTimeFormatString,
                                                               fileFormat: FileFormat = ORC)(implicit m: Manifest[T])
  extends DatePartitionedQuboleS3DataSet[T](dataSetType, S3Roots.QUBOLE_ADHOC_ROOT, rootFolderPath, partitionField, fileFormat, mergeSchema, dateTimeFormatString) {
}
