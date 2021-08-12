package com.ttd.ml.util.elDoradoUtilities.datasets.core

import com.ttd.ml.spark.TTDSparkContext
import com.ttd.ml.util.elDoradoUtilities.io.FSUtils
import org.apache.spark.sql.Dataset

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
  * DateTime partitioned dataset ("date" file path partition)
  * Allows for data append and select reading of appended data with Generation partition ("generation" file path partition)
  *
  * Ex: s3a://s3RootPath/TTDEnv/rootFolderPath/version/date=20210124/generation=20210124
  *
  * "_SUCCESS" file is written to every DateTime Generation partition
  *
  * @param dataSetType    Whether this is a Source dataset or a Generated Dataset. Source datasets are always drawn from prod sources, and are usually not written to.
  *                       Generated Datasets are given special treatment in sandbox mode, where they are written to and read from in the users personal sandbox, and
  *                       are typically written in a human-readable format for easy debugging.
  * @param s3RootPath root s3 path of the dataset, will have env append to the path before joined with rootFolderPath
  * @param rootFolderPath a relative path from the root folder to where this dataset is stored.
  * @param fileFormat format to store the dataset in
  * @param mergeSchema merge schema together, use if column added
  * @param partitions additional partitions to partition the dataset by
  * @param datePartitionName name of the date partition
  * @param dateTimeFormat format of the date and generation partition
  * @tparam T A case class containing the names and data types of all columns in this data set
  */
abstract class DateGenerationPlusPartitionedS3DataSet[T <: Product](
    dataSetType: DataSetType,
    s3RootPath: String,
    rootFolderPath: String,
    fileFormat: FileFormat = Parquet,
    mergeSchema: Boolean = false,
    partitions: Seq[(String, PartitionColumnCalculation)],
    datePartitionName: String = "date",
    generationPartitionName: String = "generation",
    dateTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(DefaultTimeFormatStrings.dateTimeFormatString)
)(implicit m: Manifest[T])
  extends S3DataSet[T](dataSetType, s3RootPath, rootFolderPath, fileFormat, mergeSchema) {

  //<editor-fold defaultstate="collapsed" desc="Write Dataset">
  /**
    * Write Dateset with Date of the dataset
    * Protected to overwrite the default method and not allow skipping date/generation partitions
    *
    * @param dataSet the dataset[T] to write to this folder. Overwrites any data that already exists in this folder.
    * @param coalesceToNumFiles Number of files to write the to
    * @param overrideSourceWrite If this is set to true, write to Source datasets. Should only ever be set for initial ETL jobs
    * @return
    */
  protected override def write(dataSet: Dataset[T], coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean = false, tempDirectory: String): Long = {
    val path = s"$writeRoot$rootFolderPath"
    if (!overrideSourceWrite && dataSetType == SourceDataSet) throw new IllegalAccessException(s"May not write data to source data set $dataSetName")
    PartitionedS3DataSet.writePartitionedDataSet(
      path = path,
      dataSet = dataSet,
      format = writeFormat,
      coalesceToNumFiles = coalesceToNumFiles,
      partitions = Seq(datePartitionName -> ColumnExistsInDataSet, generationPartitionName -> ColumnExistsInDataSet) ++ partitions,
      tempDirectory = tempDirectory
    )
  }

  /**
    * Write Dateset with Date of the dataset
    *
    * @param dataSet Dataset to write
    * @param date Date partition of data
    * @param coalesceToNumFiles Number of files to write the to
    * @return Rows written
    */
  def write(dataSet: Dataset[T], date: LocalDate, coalesceToNumFiles: Option[Int], tempDirectory: String): Long = {
    write(dataSet, date, null, coalesceToNumFiles, overrideSourceWrite = false, tempDirectory = tempDirectory)
  }

  /**
    * Write Dateset with Date and Generation partitions of the dataset
    *
    * @param dataSet Dataset to write
    * @param date Date partition of data
    * @param generation Generation partition of the data
    * @param coalesceToNumFiles Number of files to write the to
    * @return Rows written
    */
  def write(dataSet: Dataset[T], date: LocalDate, generation: LocalDate, coalesceToNumFiles: Option[Int], tempDirectory: String): Long = {
    write(dataSet, date, generation, coalesceToNumFiles, overrideSourceWrite = false, tempDirectory = tempDirectory)
  }

    /**
    * Write Dateset with Date and Generation partitions of the dataset
    *
    * @param dataSet Dataset to write
    * @param date Date partition of data
    * @param generation Generation partition of the data
    * @param coalesceToNumFiles Number of files to write the to
    * @param overrideSourceWrite Option to overwrite the source of the dataset
    * @return Rows written
    */
  def write(dataSet: Dataset[T], date: LocalDate, generation: LocalDate, coalesceToNumFiles: Option[Int], overrideSourceWrite: Boolean, tempDirectory: String): Long = {
     var path = s"$writeRoot$rootFolderPath/$datePartitionName=${date.format(dateTimeFormat)}"

     if (generation != null) path += s"/$generationPartitionName=${generation.format(dateTimeFormat)}"
     if (!overrideSourceWrite && dataSetType == SourceDataSet) throw new IllegalAccessException(s"May not write data to source data set $dataSetName")

     PartitionedS3DataSet.writePartitionedDataSet(path, dataSet, writeFormat, coalesceToNumFiles, partitions, tempDirectory)
  }
  //</editor-fold>

  //<editor-fold defaultstate="collapsed" desc="Read Dataset">
  /**
    * Read a dataset and all of it's partitions
    * WARNING: This may break or cause issues if your data is in GLacier
    *
    * @return Dataset
    */
  def read(): Dataset[T] = {
    super.read(rootFolderPath)
  }

  /**
    * Read Date partition of data
    *
    * @param dateToRead Date partition
    * @return Dataset[T]
    */
  def readDate(dateToRead: LocalDate): Dataset[T] = {
    val dateString = dateToRead.format(dateTimeFormat)
    readDate(dateString)
  }

  /**
    * Read Date partition of data
    *
    * @param dayToRead Date partition
    * @return Dataset[T]
    */
  def readDate(dayToRead: String): Dataset[T] = {
    readDateGeneration(dayToRead, null)
  }

  /**
    * Read a Date partition and Generation partition
    *
    * @param dayToRead Date partition
    * @param generationToRead Generation date, fixed.
    * @return Dataset[T]
    */
  def readDateGeneration(dayToRead: LocalDate, generationToRead: LocalDate): Dataset[T] = {
    val dateString = dayToRead.format(dateTimeFormat)
    val generationString = generationToRead.format(dateTimeFormat)
    readDateGeneration(dateString, generationString)
  }

  /**
    * Read a Date partition and Generation partition
    *
    * @param dayToRead Date partition
    * @param generationToRead Generation date, fixed.
    * @return Dataset[T]
    */
  def readDateGeneration(dayToRead: String, generationToRead: String): Dataset[T] = {
    var partitionString = s"$rootFolderPath/$datePartitionName=$dayToRead"
    if (generationToRead != null) partitionString += s"/$generationPartitionName=$generationToRead"

    read(partitionString)
  }

  /**
    * Read a range of Date partitions
    *
    * @param from Start date of range being read.
    * @param to Start date of range being read.
    * @return
    */
  def readRange(from: LocalDate, to: LocalDate): Dataset[T] = {
    readRange(
      fromDate = from.atStartOfDay(),
      toDate = to.atStartOfDay()
    )
  }

  /**
    * Read a range of Date and a Generation partition
    *
    * @param from Start date of range being read.
    * @param to Start date of range being read.
    * @param generation Generation date, fixed.
    * @return Dataset[T]
    */
  def readRange(from: LocalDate, to: LocalDate, generation: LocalDate): Dataset[T] = {
    readRange(
      fromDate = from.atStartOfDay(),
      toDate = to.atStartOfDay(),
      generation = generation
    )
  }

  /**
    * Read a range of Date partitions
    *
    * @param from Start date of range being read.
    * @param to Start date of range being read.
    * @param isInclusive Include dates in provided range.
    * @return Dataset[T]
    */
  def readRange(from: LocalDate, to: LocalDate, isInclusive:Boolean): Dataset[T] = {
    readRange(
      fromDate = from.atStartOfDay(),
      toDate = to.atStartOfDay(),
      isInclusive= isInclusive
    )
  }

  /**
    * Read a range of Date and a Generation partition
    *
    * @param from Start date of range being read.
    * @param to Start date of range being read.
    * @param generation Generation date, fixed.
    * @param isInclusive Include dates in provided range.
    * @return Dataset[T]
    */
  def readRange(from: LocalDate, to: LocalDate, generation: LocalDate, isInclusive:Boolean): Dataset[T] = {
    readRange(
      fromDate = from.atStartOfDay(),
      toDate = to.atStartOfDay(),
      generation = generation,
      isInclusive= isInclusive
    )
  }

  /**
    * Read a range of Date and optionally a Generation partition
    *
    * @param fromDate Start date of range being read.
    * @param toDate Start date of range being read.
    * @param generation (Optional) Generation date, fixed.
    * @param isInclusive Include dates in provided range.
    * @param verbose Print paths being read.
    * @return
    */
  def readRange(fromDate: LocalDateTime, toDate: LocalDateTime, generation: LocalDate = null, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    // spark does not play nice with archived files in s3. For this reason we need
    // to get and filter a list of files yourself, then pass it into spark.
    val daysToIterate =
      if (isInclusive) fromDate.until(toDate, ChronoUnit.DAYS)
      else math.max(fromDate.until(toDate, ChronoUnit.DAYS) - 1, 0)

    val generationPartitionString = if(generation != null) s"$generationPartitionName=${generation.format(dateTimeFormat)}/" else ""

    val foldersToRead = (0 to daysToIterate.toInt)
      .map(fromDate.plusDays(_).format(dateTimeFormat))
      .map(dateString => s"$readRoot/$rootFolderPath/$datePartitionName=$dateString/" + generationPartitionString)
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
      .map(_.replace(s"$readRoot/", "")) // read only needs the path past the root, so remove anything before

    if (verbose) {
      println("Reading from the following folders:")
      foldersToRead.map(folder => s"$readRoot/$folder").foreach(println)
    }

    read(foldersToRead: _*)
  }
  //</editor-fold>

  /**
    * Check if date partition exists
    *
    * @param date Date partition
    * @return
    */
  def partitionDateGenerationExists(date: LocalDate): Boolean = {
    partitionDateGenerationExists(date.format(dateTimeFormat))
  }

  /**
    * Check if date, and if provided generation, partition exists
    *
    * @param date Date partition
    * @param generation Generation date, fixed.
    * @return
    */
  def partitionDateGenerationExists(date: LocalDate, generation: LocalDate): Boolean = {
    partitionDateGenerationExists(date.format(dateTimeFormat), generation.format(dateTimeFormat))
  }

  /**
    * Check if date, and if provided generation, partition exists
    *
    * @param date Date partition
    * @param generation Generation date, fixed.
    * @return
    */
  def partitionDateGenerationExists(date: String, generation: String = null): Boolean = {
    var partitionString = s"$rootFolderPath/$datePartitionName=$date"
    if (generation != null) partitionString += s"/$generationPartitionName=$generation"

    //This can Exception if the dataset does not exist yet at all (A new dataset)
    //noinspection ScalaUnusedSymbol
    try {
      read(partitionString).head(1).nonEmpty
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case ex: org.apache.spark.sql.AnalysisException => false
      case unknown: Exception => throw unknown
    }
  }
}