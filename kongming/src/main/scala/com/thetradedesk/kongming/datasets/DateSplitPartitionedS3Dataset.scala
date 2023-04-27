package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.writeThroughHdfs
import com.thetradedesk.spark.datasets.core._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.{ProdTesting, Testing}
import com.thetradedesk.spark.util.TTDConfig.environment
import com.thetradedesk.spark.util.io.FSUtils
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet.buildPath
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{DefaultUseFirstFileSchema, SchemaPolicyType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class DateSplitPartitionedS3Dataset[T <: Product : Manifest]
(
  dataSetType: DataSetType,
  s3RootPath: String,
  rootFolderPath: String,
  fileFormat: FileFormat,
  schemaPolicy: SchemaPolicyType = DefaultUseFirstFileSchema,
)
  extends PartitionedS3DataSet2[T, LocalDate, String, String, String](
    dataSetType, s3RootPath, rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "split" -> ColumnExistsInDataSet,
    fileFormat,
    writeThroughHdfs = writeThroughHdfs,
    schemaPolicy = schemaPolicy,
  ) {

  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet

  def partitionField2: (String, PartitionColumnCalculation) = "split" -> ColumnExistsInDataSet

  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  override def toStoragePartition1(date: LocalDate): String = date.format(dateTimeFormat)

  override def toStoragePartition2(split: String): String = split

  def writePartition(dataSet: Dataset[T], partition1: LocalDate, partition2: String, coalesceToNumFiles: Option[Int]): Long = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }
    // write partition to S3 folders
    var count = super.writePartition(dataSet, partition1, partition2, coalesceToNumFiles)
    // recount the rows in partition if writePartition returns 0 and the partition actually exists
    if (count == 0 && partitionExists(partition1, partition2)) {
      count = readPartition(partition1, partition2).count()
    }
    // set environment back to ProdTesting after getting the dataset count if the environment is actually ProdTesting
    if (isProdTesting) {
      environment = ProdTesting
    }
    count
  }

  override def partitionExists(partition1: LocalDate, partition2: String): Boolean = {
    val path = buildPath(
      rootFolderPath,
      partitionField1._1 -> toStoragePartition1(partition1),
      partitionField2._1 -> toStoragePartition2(partition2)
    )
    FSUtils.directoryExists(s"$readRoot$path")
  }


  def writeDate(dataSet: Dataset[T],
                date: LocalDate,
                coalesceToNumFiles: Option[Int] = None,
                overrideSourceWrite: Boolean = false,
                cleanDestDir: Boolean = true,
                fileNameSuffix: Option[String] = None
               ): Long = {
    val datePartitionedDataSet = dataSet
      .withColumn(partitionField1._1, lit(toStoragePartition1(date)))
      .as[T]

    write(datePartitionedDataSet, coalesceToNumFiles, overrideSourceWrite, cleanDestDir, fileNameSuffix)
  }

}
