package com.thetradedesk.kongming.datasets


import com.thetradedesk.spark.datasets.core._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{DefaultUseFirstFileSchema, SchemaPolicyType}
import com.thetradedesk.kongming.EnableMetastore
import com.thetradedesk.spark.TTDSparkContext

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class ExtendedDateSplitS3Dataset[T <: Product : Manifest]
(
  dataSetType: DataSetType,
  s3RootPath: String,
  rootFolderPath: String,
  fileFormat: FileFormat,
  val extraPartitionName: String,
  val extraPartitionValue: String,
  schemaPolicy: SchemaPolicyType = DefaultUseFirstFileSchema,
  experimentOverride: Option[String] = None
)(implicit sparkSession: SparkSession = TTDSparkContext.spark)
  extends DateSplitPartitionedS3Dataset[T](
    dataSetType,
    s3RootPath,
    s"$rootFolderPath/$extraPartitionName=$extraPartitionValue/",
    fileFormat,
    schemaPolicy,
    experimentOverride
  ) {

  override val metastorePartitionField1: String = extraPartitionName
  override val metastorePartitionField2: String = "date"
  override val metastorePartitionField3: String = "split"

  override def writePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): (String, Long) = {
    val (dataSetName, count) = super.writePartition(dataSet, partition1, partition2, coalesceToNumFiles)

    (dataSetName, count)
  }

  override protected def writeMetastorePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): Unit = {
    writePartitionToMetastore(dataSet, extraPartitionValue, partition1, partition2, coalesceToNumFiles)
  }

  override protected def registerMetastorePartition(date: LocalDate, split: String): Unit = {
    registerPartitionToMetastore(extraPartitionValue, date, split)
  }

  override def readStoragePartition(value: String): Dataset[T] = {
    println(s"[readStoragePartition1] Read partition of $value")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readPartitionFromMetastore(extraPartitionValue, value), schema)
    }
    else {
      super.readStoragePartition(value)
    }
  }

  override def readStoragePartition(value1: String, value2: String): Dataset[T] = {
    println(s"[readPartition2] Read partition of $value1, $value2")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readPartitionFromMetastore(extraPartitionValue, value1, value2), schema)
    }
    else {
      super.readStoragePartition(value1, value2)
    }
  }

  override def readLatestPartition(verbose: Boolean = true): Dataset[T] = {
    println(s"[readLatestPartition] ...")
    if (shouldUseMetastoreForReadAndWrite()) {
      import spark.implicits._
      autoRenameAndAs[T](readLatestPartitionFromMetastore(extraPartitionValue), schema)
    }
    else {
      super.readLatestPartition(verbose=true)
    }
  }
}

