package com.ttd.contextual.datasets.core

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{DataSetType, FileFormat, Parquet}
import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.{ColumnExistsInDataSet, DataSetType, DefaultTimeFormatStrings, FileFormat, Parquet, PartitionedS3DataSet2}
import com.ttd.contextual.util.elDoradoUtilities.io.FSUtils
import com.ttd.contextual.spark.TTDSparkContext
import org.apache.spark.sql.Dataset

abstract class SourceDatePartitionedS3DataSet[T <: Product](
                                                             dataSetType: DataSetType,
                                                             s3RootPath: String,
                                                             rootFolderPath: String,
                                                             sourceField: String,
                                                             dateField: String,
                                                             fileFormat: FileFormat = Parquet,
                                                             mergeSchema: Boolean = false,
                                                             dateTimeFormatString: String = DefaultTimeFormatStrings.dateTimeFormatString)
                                                           (implicit m: Manifest[T])
  extends PartitionedS3DataSet2[T, String, String, LocalDate, String](
    dataSetType,
    s3RootPath,
    rootFolderPath,
    sourceField -> ColumnExistsInDataSet,
    dateField -> ColumnExistsInDataSet,
    fileFormat,
    mergeSchema
  ) {

  override def toStoragePartition1(value: String): String = value

  override def toStoragePartition2(value: LocalDate): String = {
    value.format(dateTimeFormat)
  }

  def dateTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatString)

  def readSourceAndDate(source: String, date: String): Dataset[T] = {
    read(s"$rootFolderPath/${sourceField}=$source/${dateField}=$date/")
  }

  def readSourceAndDate(source: String, dateToRead: LocalDate): Dataset[T] = {
    val dateString = dateToRead.format(dateTimeFormat)
    readSourceAndDate(source, dateString)
  }

  def readSourceAndRange(source: String, fromDate: LocalDateTime, toDate: LocalDateTime, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    // spark does not play nice with archived files in s3. For this reason we need
    // to get and filter a list of files yourself, then pass it into spark.
    val daysToIterate =
    if (isInclusive) fromDate.until(toDate, ChronoUnit.DAYS)
    else math.max(fromDate.until(toDate, ChronoUnit.DAYS) - 1, 0)

    val foldersToRead = (0 to daysToIterate.toInt)
      .map(fromDate.plusDays(_).format(dateTimeFormat))
      .map(dateString => s"$readRoot/$rootFolderPath/${sourceField}=$source/$dateField=$dateString/")
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
      .map(_.replace(s"$readRoot/", "")) // read only needs the path past the root, so remove anything before

    if (verbose) {
      println("Reading from the following folders:")
      foldersToRead.map(folder => s"$readRoot/$folder").foreach(println)
    }

    read(foldersToRead: _*)
  }

}

