package com.ttd.ml.util.elDoradoUtilities.datasets.core

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.regex.Pattern

import com.ttd.ml.util.elDoradoUtilities.logging.Logger
import com.ttd.ml.spark.TTDSparkContext
import com.ttd.ml.spark.TTDSparkContext.spark
import com.ttd.ml.spark.TTDSparkContext.spark.implicits._
import com.ttd.ml.util.elDoradoUtilities.datasets.core.DataSetMode.DataSetMode
import com.ttd.ml.util.elDoradoUtilities.spark.listener.WriteListener
import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.ml.util.elDoradoUtilities.spark.TTDConfig
import com.ttd.ml.util.elDoradoUtilities.spark._
import com.ttd.ml.util.elDoradoUtilities.distcp.{AppendFoldersToExisting, DataOutputUtils}
import com.ttd.ml.util.elDoradoUtilities.io.FSUtils
import com.ttd.ml.util.elDoradoUtilities.spark.RichLocalDate._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode}

import scala.util.Try

sealed trait PartitionColumnCalculation

case object ColumnExistsInDataSet extends PartitionColumnCalculation

case class CalculateAs(expression: Column) extends PartitionColumnCalculation

/**
 * If you need to change any logic here, remember to also make sure that S3DataSet also gets changes, to maintain logical parity
 */
object PartitionedS3DataSet extends Logger {
  def writePartitionedDataSet[T](path: String, dataSet: Dataset[T], format: FileFormat, coalesceToNumFiles: Option[Int], partitions: Seq[(String, PartitionColumnCalculation)], tempDirectory: String): Long = {
    log.info(s"Writing partitioned dataset (${partitions.map(_._1).mkString(",")}) to $path as $format with $coalesceToNumFiles file partitions")
    // For each calculated partition, add it as a column so we can partition by
    val dataFrameWithCalculatedColumns = partitions.foldLeft(dataSet.toDF()) {
      case (dataFrame, (_, ColumnExistsInDataSet)) => dataFrame
      case (dataFrame, (fieldName, CalculateAs(expression: Column))) => dataFrame.withColumn(fieldName, expression)
    }

    val partitionedDataset = coalesceToNumFiles match {
      case Some(numBuckets) => dataFrameWithCalculatedColumns.repartition(numBuckets)
      case None => dataFrameWithCalculatedColumns
    }

    def writer(dataset: DataFrame) = dataset
      .write // this doesn't actually write - just creates the writer
      .partitionBy(partitions.map(_._1): _*) // partition by the field names we either know exist or created
      .mode(SaveMode.Append)

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    val result = Try {
      // write out in the format of our choice
      val dataWriterFunction: String => Unit = format match {
        case Parquet =>
          pathstr => writer(partitionedDataset).parquet(pathstr)
        case Json =>
          pathstr => writer(partitionedDataset).json(pathstr)
        case Csv(withHeader) =>
          pathstr => writer(partitionedDataset).option("header", withHeader.toString).csv(pathstr)
        case Tsv(withHeader) =>
          pathstr => writer(partitionedDataset).options(Map("sep" -> "\t", "header" -> withHeader.toString)).csv(pathstr)
        case ORC =>
          pathstr => writer(partitionedDataset).orc(pathstr)
      }
      if (TTDConfig.environment == Local
        || TTDConfig.environment == LocalParquet
        || TTDConfig.environment == LocalORC
        || tempDirectory.equals("")) {
        dataWriterFunction(path) // Just write the data
      } else {
        // If we're on EMR, write to HDFS then copy to Spark when completed.
        DataOutputUtils.writeToS3ViaHdfsInterning(
          path,
          dataWriterFunction,
          AppendFoldersToExisting,
          throwIfSourceEmpty = true,
          tempDirectory = tempDirectory
        )(spark)
      }

      listener.rowsWritten
    }

    // remove listener, get stats
    spark.sparkContext.removeSparkListener(listener)

    result.get
  }

  def buildPath(rootFolderPath: String, partitionsAndValues: (String, String)*):String = {
    val partitionPaths = partitionsAndValues.map(pv => s"${pv._1}=${pv._2}").mkString("/")
    s"$rootFolderPath/$partitionPaths"
  }

  def buildPartitionPattern(partitionNames: String*): Pattern = {
    val partitionPatternString = partitionNames.map(p => s"$p=(.*?)")
      .mkString("/")
    Pattern.compile(s"/$partitionPatternString(?:/|$$)")
  }

  def getPartitionFolders(readRoot: String, rootFolderPath: String, depth: Int): Seq[String] = {
    var allFolders = FSUtils.listDirectoryContents(s"$readRoot/$rootFolderPath")(spark)
      .filter(_.isDirectory)

    for(_ <- 0 until depth) {
      allFolders = allFolders
        .flatMap(path => FSUtils.listDirectoryContents(path.getPath.toString)(spark))
        .filter(_.isDirectory)
    }

    allFolders.map(_.getPath.toString.replace(s"$readRoot", ""))
  }
}

abstract class PartitionedS3DataSet[T <: Product](
                                                   dataSetType: DataSetType,
                                                   s3RootPath: String,
                                                   rootFolderPath: String,
                                                   partitions: Seq[(String, PartitionColumnCalculation)],
                                                   fileFormat: FileFormat = Parquet,
                                                   mergeSchema: Boolean = false)
                                                 (implicit m: Manifest[T])
  extends S3DataSet[T](dataSetType, s3RootPath, rootFolderPath, fileFormat, mergeSchema) {
  override def write(dataSet: Dataset[T], coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean = false, tempDirectory: String): Long = {
    write(dataSet, coalesceToNumFiles, overrideSourceWrite, this.partitions, tempDirectory)
  }

  protected def write(dataSet: Dataset[T], coalesceToNumFiles: Option[Int], overrideSourceWrite: Boolean, partitions: Seq[(String, PartitionColumnCalculation)], tempDirectory: String): Long = {
    val path = s"$writeRoot$rootFolderPath"
    if (!overrideSourceWrite && dataSetType == SourceDataSet) throw new IllegalAccessException(s"May not write data to source data set $dataSetName")
    PartitionedS3DataSet.writePartitionedDataSet(path, dataSet, writeFormat, coalesceToNumFiles, partitions, tempDirectory)
  }

  /**
   * Read a dataset and all of it's partitions
   * WARNING: This may break or cause issues if your data is in GLacier
   *
   * @return Dataset
   */
  def read(): Dataset[T] = {
    super.read(rootFolderPath)
  }
}

abstract class PartitionedS3DataSet1[T <: Product, TDataSetPartition, TStoragePartition](
                                                                                          dataSetType: DataSetType,
                                                                                          s3RootPath: String,
                                                                                          rootFolderPath: String,
                                                                                          partitionField: (String, PartitionColumnCalculation),
                                                                                          fileFormat: FileFormat,
                                                                                          mergeSchema: Boolean = false)
                                                                                        (implicit m: Manifest[T])
  extends PartitionedS3DataSet[T](dataSetType, s3RootPath, rootFolderPath, Seq(partitionField), fileFormat, mergeSchema) {

  def toStoragePartition(value: TDataSetPartition): TStoragePartition

  /** Reads a single partition
   *
   * @param value the partition id
   **/
  def readPartition(value: TDataSetPartition): Dataset[T] = {
    readPartition(toStoragePartition(value))
  }

  protected def readPartition(value: TStoragePartition)(implicit i1: DummyImplicit): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField._1 -> value.toString
    ))
  }

  /**
   * Check to see if the partition exists on s3
   *
   * @param value the partition id
   */
  def partitionExists(value: TDataSetPartition): Boolean = {
    // Filter for the partition in question and check if it exists
    try {
      readPartition(value).head(1).nonEmpty
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case ex: org.apache.spark.sql.AnalysisException => {
        false
      }
      case unknown: Exception => {
        throw unknown
      }
    }
  }

  /**
   * Finds the latest existing partition ordered by its partition name.
   *
   * @return
   */
  val partitionPattern: Pattern = PartitionedS3DataSet.buildPartitionPattern(partitionField._1)
  def readLatestPartition(verbose: Boolean = true): Dataset[T] = {
    val allFolders = PartitionedS3DataSet.getPartitionFolders(readRoot, rootFolderPath, depth = 0)
    val max = allFolders.flatMap { folder =>
      val matcher = partitionPattern.matcher(folder)
      if(matcher.find()) {
        Some(matcher.group(1))
      }
      else None
    }.max

    val folderToRead = s"$rootFolderPath/${partitionField._1}=$max"
    if (verbose) {
      println(s"Reading from $folderToRead")
    }

    read(folderToRead)
  }

  /**
   * This function finds and returns  the latest date partition up to (and possibly including) maxInclusivePartition.
   * Our current parquet sync job don't always have hourly partition created, by setting, and by possible timing
   * of the execution.  So to be able to consistently find latest date partition without knowing exactly
   * what partitions are available, use this function.
   */
  def readLatestPartitionUpTo(maxInclusivePartition: TDataSetPartition, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    val allFolders = PartitionedS3DataSet.getPartitionFolders(readRoot, rootFolderPath, depth = 0)
    val max = allFolders.flatMap { folder =>
      val matcher = partitionPattern.matcher(folder)
      if(matcher.find()) matcher.group(1) match {
        case matchInclusive if isInclusive && matchInclusive <= toStoragePartition(maxInclusivePartition).toString => Some(matchInclusive)
        case matchNotInclusive if !isInclusive && matchNotInclusive < toStoragePartition(maxInclusivePartition).toString => Some(matchNotInclusive)
        case _ => None
      }
      else None
    }.max

    val folderToRead = s"$rootFolderPath/${partitionField._1}=$max"
    if (verbose) {
      println(s"Reading from $folderToRead")
    }

    read(folderToRead)
  }

  // going over scala limitation on repeated optional parameters for overloads
  def writePartition(dataSet: Dataset[T], partition: TDataSetPartition, fileCount: Int, tempDirectory: String): Long = {
    writePartition(dataSet, partition, Some(fileCount), tempDirectory = tempDirectory)
  }

  def writePartition(dataSet: Dataset[T], partition: TDataSetPartition, fileCount: Int, overrideSourceWrite: Boolean, tempDirectory: String): Long = {
    writePartition(dataSet, partition, Some(fileCount), overrideSourceWrite, tempDirectory)
  }

  def writePartition(dataSet: Dataset[T], partition: TDataSetPartition, coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean = false, tempDirectory: String): Long = {
    // define partition as constant value
    val partitions = Seq((partitionField._1, CalculateAs(lit(toStoragePartition(partition)))))
    write(dataSet, coalesceToNumFiles, overrideSourceWrite, partitions, tempDirectory)
  }
}

abstract class PartitionedS3DataSet2[T <: Product, TPartition1, TStoragePartition1, TPartition2, TStoragePartition2](
                                                                                                                      dataSetType: DataSetType,
                                                                                                                      s3RootPath: String,
                                                                                                                      rootFolderPath: String,
                                                                                                                      partitionField1: (String, PartitionColumnCalculation),
                                                                                                                      partitionField2: (String, PartitionColumnCalculation),
                                                                                                                      fileFormat: FileFormat,
                                                                                                                      mergeSchema: Boolean = false)
                                                                                                                    (implicit m: Manifest[T])
  extends PartitionedS3DataSet[T](dataSetType, s3RootPath, rootFolderPath, Seq(partitionField1, partitionField2), fileFormat, mergeSchema) {

  def toStoragePartition1(value1: TPartition1): TStoragePartition1

  def toStoragePartition2(value2: TPartition2): TStoragePartition2

  /** Reads a single partition
   *
   * @param value the partition id */
  def readStoragePartition(value: TStoragePartition1): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField1._1 -> value.toString
    ))
  }

  def readPartition(partition: TPartition1): Dataset[T] = {
    readStoragePartition(toStoragePartition1(partition))
  }

  /** Reads a single partition
   *
   * @param value1 first partition
   * @param value2 second partition */
  def readStoragePartition(value1: TStoragePartition1, value2: TStoragePartition2): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField1._1 -> value1.toString,
      partitionField2._1 -> value2.toString
    ))
  }

  /** Reads a single partition
   *
   * @param value1 first partition
   * @param value2 second partition */
  def readPartition(value1: TPartition1, value2: TPartition2): Dataset[T] = {
    readStoragePartition(toStoragePartition1(value1), toStoragePartition2(value2))
  }

  /**
   * This function reads the latest partition in the PartitionField2 in the latest partition in PartitionField1,
   * ordered by its partition names.
   *
   * @return
   */
  val partitionPattern: Pattern = PartitionedS3DataSet.buildPartitionPattern(partitionField1._1, partitionField2._1)
  def readLatestPartition(verbose: Boolean = true): Dataset[T] = {
    val allFolders = PartitionedS3DataSet.getPartitionFolders(readRoot, rootFolderPath, depth = 1)
    val max = allFolders.flatMap { folder =>
      val matcher = partitionPattern.matcher(folder)
      if(matcher.find()) {
        Some(matcher.group(1) -> matcher.group(2))
      }
      else None
    }.max

    val folderToRead = s"$rootFolderPath/${partitionField1._1}=${max._1}/${partitionField2._1}=${max._2}"
    if (verbose) {
      println(s"Reading from $folderToRead")
    }

    read(folderToRead)
  }

  def writePartition(dataSet: Dataset[T], partition1: TPartition1, partition2: TPartition2, coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean = false, tempDirectory: String): Long = {
    // define partition as constant value
    val partitions = Seq(
      (partitionField1._1, CalculateAs(lit(toStoragePartition1(partition1)))),
      (partitionField2._1, CalculateAs(lit(toStoragePartition2(partition2)))))
    write(dataSet, coalesceToNumFiles, overrideSourceWrite, partitions, tempDirectory)
  }

  /**
   * Check to see if the partition exists on s3
   *
   * @param value the partition id
   */
  def partitionExists(value: TPartition1): Boolean = {
    // Filter for the partition in question and check if it exists
    try {
      readPartition(value).head(1).nonEmpty
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case ex: org.apache.spark.sql.AnalysisException => {
        false
      }
      case unknown: Exception => {
        throw unknown
      }
    }
  }

  /**
   * Check to see if the partition exists on s3
   *
   * @param value1 the partition 1 id
   * @param value2 the partition 2 id
   */
  def partitionExists(value1: TPartition1, value2: TPartition2): Boolean = {
    // Filter for the partition in question and check if it exists
    try {
      readPartition(value1, value2).head(1).nonEmpty
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case ex: org.apache.spark.sql.AnalysisException => {
        false
      }
      case unknown: Exception => {
        throw unknown
      }
    }
  }

}

abstract class PartitionedS3DataSet3[T <: Product, TPartition1, TPartition2, TPartition3](
                                                                                           dataSetType: DataSetType,
                                                                                           s3RootPath: String,
                                                                                           rootFolderPath: String,
                                                                                           partitionField1: (String, PartitionColumnCalculation),
                                                                                           partitionField2: (String, PartitionColumnCalculation),
                                                                                           partitionField3: (String, PartitionColumnCalculation),
                                                                                           fileFormat: FileFormat = Parquet,
                                                                                           mergeSchema: Boolean = false)
                                                                                         (implicit m: Manifest[T])
  extends PartitionedS3DataSet[T](
    dataSetType,
    s3RootPath,
    rootFolderPath,
    Seq(partitionField1, partitionField2, partitionField3),
    fileFormat,
    mergeSchema) {

  /** Reads a single partition
   *
   * @param value the partition id */
  def readPartition(value: TPartition1): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField1._1 -> value.toString
    ))
  }

  /** Reads a single partition
   *
   * @param value1 first partition
   * @param value2 second partition */
  def readPartition(value1: TPartition1, value2: TPartition2): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField1._1 -> value1.toString,
      partitionField2._1 -> value2.toString
    ))
  }

  /** Reads a single partition
   *
   * @param value1 first partition
   * @param value2 second partition
   * @param value3 third partition */
  def readPartition(value1: TPartition1, value2: TPartition2, value3: TPartition3): Dataset[T] = {
    read(PartitionedS3DataSet.buildPath(rootFolderPath,
      partitionField1._1 -> value1.toString,
      partitionField2._1 -> value2.toString,
      partitionField2._1 -> value3.toString
    ))
  }

  /**
   * Check to see if the partition exists on s3
   *
   * @param value the partition id
   */
  def partitionExists(value: TPartition1): Boolean = {
    // Filter for the partition in question and check if it exists
    try {
      readPartition(value).head(1).nonEmpty
    } catch {
      // If no partitions exist at all, it will error attempting to filter
      case ex: org.apache.spark.sql.AnalysisException => {
        false
      }
      case unknown: Exception => {
        throw unknown
      }
    }
  }

  def writePartition(dataSet: Dataset[T], value1: TPartition1, value2: TPartition2, value3: TPartition3, coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean, tempDirectory: String): Unit = {
    // define partition as constant value
    val partitions = Seq((partitionField1._1, CalculateAs(lit(value1))), (partitionField2._1, CalculateAs(lit(value2))), (partitionField3._1, CalculateAs(lit(value3))))
    write(dataSet, coalesceToNumFiles, overrideSourceWrite, partitions, tempDirectory)
  }
}

object DefaultTimeFormatStrings {
  val dateTimeFormatString: String = "yyyyMMdd" // For some reason, if this has -'s in it it reads back in as nulls.
  lazy val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatString)
  val dateTimeHourFormatString: String = "yyyyMMddHH"
  lazy val dateTimeHourFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeHourFormatString)
  val dateTimeFormatStringWithDashes: String = "yyyy-MM-dd" // For some reason, if this has -'s in it it reads back in as nulls.
  lazy val dateTimeFormatterWithDashes: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatStringWithDashes)
  val hourTimeFormatString: String = "HH"
}

/*
 * This class implements an interface to read parquet files from S3 that parititioned by date and hour.
 * See above for the format of the partition name.
 * This class has two constructors.
 * One takes the columns for date and hour,
 * one takes a column in the table that the constructor derive date and hour columns.
 */
case class DateHourFormatStrings(dateTimeFormatString: String, hourTimeFormatString: String)

case class DateHour(date: String, hour: Int)

abstract class DateHourPartitionedS3DataSet[T <: Product](
                                                           dataSetType: DataSetType,
                                                           s3RootPath: String,
                                                           rootFolderPath: String,
                                                           dateField: (String, PartitionColumnCalculation),
                                                           hourField: (String, PartitionColumnCalculation),
                                                           fileFormat: FileFormat = Parquet,
                                                           mergeSchema: Boolean = false,
                                                           formatStrings: DateHourFormatStrings = DateHourFormatStrings(
                                                             DefaultTimeFormatStrings.dateTimeFormatString,
                                                             DefaultTimeFormatStrings.hourTimeFormatString))
                                                         (implicit m: Manifest[T])
  extends PartitionedS3DataSet2[T, LocalDate, String, Int, Int](
    dataSetType,
    s3RootPath,
    rootFolderPath,
    dateField,
    hourField,
    fileFormat,
    mergeSchema
  ) {

  def this(dataSetType: DataSetType,
           s3RootPath: String,
           rootFolderPath: String,
           timestampFieldName: String,
           fileFormat: FileFormat,
           mergeSchema: Boolean,
           formatStrings: DateHourFormatStrings)
          (implicit m: Manifest[T]) {
    this(dataSetType,
      s3RootPath,
      rootFolderPath,
      ("date", CalculateAs(date_format(col(timestampFieldName), formatStrings.dateTimeFormatString))),
      ("hour", CalculateAs(date_format(col(timestampFieldName), formatStrings.hourTimeFormatString))),
      fileFormat,
      mergeSchema,
      formatStrings)
  }

  override def toStoragePartition1(value: LocalDate): String = {
    value.format(dateTimeFormat)
  }

  override def toStoragePartition2(value: Int): Int = value

  val dateTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(formatStrings.dateTimeFormatString)
  val hourTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(formatStrings.hourTimeFormatString)
  val dateHourFolderPathRegex: Pattern = Pattern.compile(s"/${dateField._1}=(.*?)/${hourField._1}=(.*?)(?:/|$$)")

  def readDate(date: LocalDate): Dataset[T] = {
    readDate(date.format(dateTimeFormat))
  }

  def readDate(date: String): Dataset[T] = {
    read(s"$rootFolderPath/${dateField._1}=$date/")
  }

  def readHour(dateTime: LocalDateTime): Dataset[T] = {
    readHour(dateTime.format(dateTimeFormat), dateTime.format(hourTimeFormat).toInt)
  }

  def readHour(date: String, hour: Int): Dataset[T] = {
    read(s"$rootFolderPath/${dateField._1}=$date/${hourField._1}=${hour.toString.reverse.padTo(formatStrings.hourTimeFormatString.length, '0').reverse}")
  }

  /**
   * Reads a range of partitions
   */
  def readRange(fromTime: LocalDateTime, toTime: LocalDateTime, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    // spark does not play nice with archived files in s3. For this reason we need
    // to get and filter a list of files yourself, then pass it into spark.
    val hoursToIterate =
    if (isInclusive) fromTime.until(toTime, ChronoUnit.HOURS)
    else Math.max(fromTime.until(toTime, ChronoUnit.HOURS) - 1, 0)

    val foldersToRead = (0 to hoursToIterate.toInt)
      .map { currentHour =>
        val time = fromTime.plusHours(currentHour)
        s"$readRoot/$rootFolderPath/${dateField._1}=${time.format(dateTimeFormat)}/${hourField._1}=${time.format(hourTimeFormat)}"
      }
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
      .map(_.replace(s"$readRoot/", "")) // read only needs the path past the root, so remove anything before

    if (verbose) {
      println("Reading from the following folders:")
      foldersToRead.map(folder => s"$readRoot/$folder").foreach(println)
    }

    read(foldersToRead: _*)
  }

  def readRange(from: Timestamp, to: Timestamp): Dataset[T] = {
    readRange(from.toLocalDateTime, to.toLocalDateTime)
  }

  /**
   * This function finds and returns  the latest date/hour partition up to now.
   * Our current parquet sync job don't always have hourly partition created, by setting, and by possible timing
   * of the execution.  So to be able to consistently find latest date/hour parition without knowing exactly
   * what partitions are available, use this function.
   */
  def readLatestDateHourPartition(verbose: Boolean = false): Dataset[T]
  = this.readLatestPartitionUpToDateHour(LocalDateTime.now, verbose = verbose)

  /**
   * This function finds and returns the latest date/hour partition up to upToDateHour(LocalDateTime).
   * Our current parquet sync job don't always have hourly partition created, by setting, and by possible timing
   * of the execution.  So to be able to consistently find latest date/hour parition without knowing exactly
   * what partitions are available, use this function.
   */
  def readLatestPartitionUpToDateHour(upToDateHour: LocalDateTime, verbose: Boolean = false, isInclusive: Boolean = false): Dataset[T] = {
    val maxDateString = upToDateHour.toLocalDate.format(dateTimeFormat)
    val maxHourInt = upToDateHour.toLocalTime.format(hourTimeFormat).toInt

    // gotta look 2 folders deep for date/hour
    val allFolders = FSUtils.listDirectoryContents(s"$readRoot/$rootFolderPath")(spark)
      .filter(_.isDirectory)
      .flatMap(path => FSUtils.listDirectoryContents(path.getPath.toString)(spark))
      .map(_.getPath.toString.replace(s"$readRoot", ""))

    val (bestDate, bestHour) = allFolders
      .flatMap { folder =>
        val matcher = dateHourFolderPathRegex.matcher(folder)
        if (matcher.find()) {
          val date = matcher.group(1)
          val hour = matcher.group(2)
          if ((date < maxDateString) || (date == maxDateString && (hour.toInt < maxHourInt || (isInclusive && hour.toInt == maxHourInt))))
            Some(date -> hour)
          else None
        }
        else None
      }
      .max

    val folderToRead = s"$rootFolderPath/${dateField._1}=$bestDate/${hourField._1}=$bestHour"
    if (verbose) {
      println(s"Reading from $folderToRead")
    }

    read(folderToRead)
  }

  /**
   * This function finds and returns  the latest date/hour partition up to upToDate(LocalDate).
   * Our current parquet sync job don't always have hourly partition created, by setting, and by possible timing
   * of the execution.  So to be able to consistently find latest date/hour parition without knowing exactly
   * what partitions are available, use this function.
   */
  def readLatestPartitionUpToDate(upToDate: LocalDate, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] =
    if (isInclusive) readLatestPartitionUpToDateHour(upToDate.atTime(LocalTime.MAX), isInclusive = true)
    else readLatestPartitionUpToDateHour(upToDate.atStartOfDay(), isInclusive = false)
}

abstract class DatePartitionedS3DataSet[T <: Product](
                                                       dataSetType: DataSetType,
                                                       s3RootPath: String,
                                                       rootFolderPath: String,
                                                       partitionField: String = "date",
                                                       fileFormat: FileFormat = Parquet,
                                                       mergeSchema: Boolean = false,
                                                       dateTimeFormatString: String = DefaultTimeFormatStrings.dateTimeFormatString)
                                                     (implicit m: Manifest[T])
  extends PartitionedS3DataSet1[T, LocalDate, String](
    dataSetType,
    s3RootPath,
    rootFolderPath,
    partitionField -> ColumnExistsInDataSet,
    fileFormat,
    mergeSchema) {

  def dateTimeFormat: DateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatString)

  def toStoragePartition(value: LocalDate): String = {
    value.format(dateTimeFormat)
  }

  def readRange(from: LocalDate, to: LocalDate): Dataset[T] = {
    readRange(from.atStartOfDay(), to.atStartOfDay())
  }

  def readRange(from: LocalDate, to: LocalDate, isInclusive:Boolean): Dataset[T] = {
    readRange(from.atStartOfDay(), to.atStartOfDay(), isInclusive)
  }

  // todo: cjo - define these in base class, in terms of TDataSetPartition
  /**
   * Reads a range of partitions.
   */
  def readRange(fromDate: LocalDateTime, toDate: LocalDateTime, isInclusive: Boolean = false, verbose: Boolean = false): Dataset[T] = {
    // spark does not play nice with archived files in s3. For this reason we need
    // to get and filter a list of files yourself, then pass it into spark.
    val daysToIterate =
    if (isInclusive) fromDate.until(toDate, ChronoUnit.DAYS)
    else math.max(fromDate.until(toDate, ChronoUnit.DAYS) - 1, 0)

    val foldersToRead = (0 to daysToIterate.toInt)
      .map(fromDate.plusDays(_).format(dateTimeFormat))
      .map(dateString => s"$readRoot/$rootFolderPath/$partitionField=$dateString/")
      .filter(FSUtils.directoryExists(_)(TTDSparkContext.spark))
      .map(_.replace(s"$readRoot/", "")) // read only needs the path past the root, so remove anything before

    if (verbose) {
      println("Reading from the following folders:")
      foldersToRead.map(folder => s"$readRoot/$folder").foreach(println)
    }

    read(foldersToRead: _*)
  }

  def readDate(dateToRead: LocalDate): Dataset[T] = {
    val dateString = dateToRead.format(dateTimeFormat)
    readDate(dateString)
  }

  def readDate(dayToRead: String): Dataset[T] = {
    read(s"$rootFolderPath/$partitionField=$dayToRead/")
  }
}
