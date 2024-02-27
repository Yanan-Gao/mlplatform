package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet
import com.thetradedesk.spark.datasets.core.SchemaPolicy.{DefaultUseFirstFileSchema, SchemaPolicyType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import com.thetradedesk.spark.datasets.core._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

// write data class: need the experimentOverride}

// write data class: need the experimentOverride
final case class TrainDataBalancedRecord(
                                          BidRequestId: String,
                                          LogEntryTime:  java.sql.Timestamp,

                                          biddate: String,

                                          ConfigKey: String,
                                          ConfigValue: String,
                                          Weight: Double,
                                          Target: Int,
                                          Revenue: Option[BigDecimal],
                                          IsInTrainSet: Boolean
                                        ){}


case class IntermediateTrainDataBalancedDataset(experimentOverride: Option[String] = None) extends PartitionedS3DataSet[TrainDataBalancedRecord](
  dataSetType = GeneratedDataSet,
  s3RootPath = MLPlatformS3Root,
  rootFolderPath = s"${BaseFolderPath}/intermediatetraindatabalanceddataset/v=1",
  Seq(),
    fileFormat = Parquet,
    experimentOverride = experimentOverride
)  {
  // intermediate dataset of trainset gen, generate this dataset only for speeding job up purpose
  def dateTimeFormat: DateTimeFormatter = DefaultTimeFormatStrings.dateTimeFormatter

  def writePartition(
                      dataSet: Dataset[TrainDataBalancedRecord],
                      date: LocalDate,
                      partitionBy: String,
                      coalesceToNumFiles: Option[Int],
                      overrideSourceWrite: Boolean = true,
                      cleanDestDir: Boolean = true,
                      fileNameSuffix: Option[String] = None
                    ): Long = {
    val partitions = Seq(
      ("date", CalculateAs(lit(date.format(dateTimeFormat)))),
      (partitionBy, ColumnExistsInDataSet))
    write(dataSet, coalesceToNumFiles, overrideSourceWrite, partitions, cleanDestDir, fileNameSuffix)
  }

  def readPartition(date: LocalDate, partitionBy: String, partitionValue: String): Dataset[TrainDataBalancedRecord] = {
    val dateFormatted = date.format(dateTimeFormat)
    read(s"$rootFolderPath/date=$dateFormatted/$partitionBy=$partitionValue")
  }

  def readDate(date: LocalDate): Dataset[TrainDataBalancedRecord] = {
    val dateFormatted = date.format(dateTimeFormat)
    read(s"$rootFolderPath/date=$dateFormatted")
  }
}