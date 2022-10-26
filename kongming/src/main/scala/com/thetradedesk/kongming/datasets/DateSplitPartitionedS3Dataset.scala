package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.writeThroughHdfs
import com.thetradedesk.spark.datasets.core._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class DateSplitPartitionedS3Dataset[T <: Product : Manifest]
(
  dataSetType: DataSetType,
  s3RootPath: String,
  rootFolderPath: String,
  fileFormat: FileFormat,
  mergeSchema: Boolean = false,
)
  extends PartitionedS3DataSet2[T, LocalDate, String, String, String](
    dataSetType, s3RootPath, rootFolderPath,
    "date" -> ColumnExistsInDataSet,
    "split" -> ColumnExistsInDataSet,
    fileFormat,
    mergeSchema,
    writeThroughHdfs
  ) {

  def partitionField1: (String, PartitionColumnCalculation) = "date" -> ColumnExistsInDataSet
  def partitionField2: (String, PartitionColumnCalculation) = "split" -> ColumnExistsInDataSet

  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  override def toStoragePartition1(date: LocalDate): String = date.format(dateTimeFormat)
  override def toStoragePartition2(split: String): String = split


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
