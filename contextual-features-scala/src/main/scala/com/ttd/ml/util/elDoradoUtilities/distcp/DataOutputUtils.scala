package com.ttd.ml.util.elDoradoUtilities.distcp

/* NOTE:
 * This is a direct copy from the Identity teams neocortex library. At first chance, this should be replaced with
 * a dependency on the library in question.
 */

import java.io.IOException
import java.util.UUID

import com.ttd.ml.util.elDoradoUtilities.logging.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.ttd.ml.util.elDoradoUtilities.io.FSUtils

trait CopyType
object OverwriteExisting extends CopyType
object CopyToEmpty extends CopyType
object AppendFoldersToExisting extends CopyType

/**
 * Exposes methods for specialized outputting of data frames
 */
object DataOutputUtils extends Logger {

  /**
   * Saves the data frame into tsv file by optionally sorting the rows by the given column(s)
   * @param data - data frame
   * @param filePath - file path
   * @param writeHeader - value indicating if the column names should be written to the file
   * @param sortColumns - sort columns [optional]
   */
  def writeAsSingleTsv(data: DataFrame, filePath: String, writeHeader: Boolean, sortColumns: Seq[String] = Seq())
                      (implicit spark: SparkSession) : Unit = {
    writeAsSingleFile(data, filePath, writeHeader, "\t", sortColumns)
  }

  /**
   * Saves the data frame into csv file by optionally sorting the rows by the given column(s)
   * @param data - data frame
   * @param filePath - file path
   * @param writeHeader - value indicating if the column names should be written to the file
   * @param sortColumns - sort columns [optional]
   */
  def writeAsSingleCsv(data: DataFrame, filePath: String, writeHeader: Boolean, sortColumns: Seq[String] = Seq())
                      (implicit spark: SparkSession) : Unit = {
    writeAsSingleFile(data, filePath, writeHeader, ",", sortColumns)
  }

  /**
   * Writes the data frame as parquet to hdfs then copied to s3
   * @param data data frame to output
   * @param outputS3Path output path on s3
   * @param copyType value indicating if existing non-empty output directory should be overwritten
   * @param tempDirectory base temporary directory
   * @param spark spark session
   */
  def writeToS3AsParquetViaHdfsInterning(data: DataFrame,
                                         outputS3Path: String,
                                         copyType: CopyType = OverwriteExisting,
                                         tempDirectory: String = "hdfs:///user/hadoop/output-temp-dir")
                                        (implicit spark: SparkSession) : Unit = {
    writeToS3ViaHdfsInterning(
      outputS3Path,
      hdfsTempPath => {
        data.write.parquet(hdfsTempPath)
      },
      copyType,
      tempDirectory)
  }

  /**
   * Writes the data frame as csv to hdfs then copied to s3
   * @param data data frame to output
   * @param outputS3Path output path on s3
   * @param writeHeader value indicating if the header should be written
   * @param copyType value indicating if existing non-empty output directory should be overwritten
   * @param tempDirectory base temporary directory
   * @param spark spark session
   */
  def writeToS3AsCsvViaHdfsInterning(data: DataFrame,
                                     outputS3Path: String,
                                     writeHeader: Boolean = false,
                                     copyType: CopyType = OverwriteExisting,
                                     tempDirectory: String = "hdfs:///user/hadoop/output-temp-dir")
                                    (implicit spark: SparkSession) : Unit = {
    writeToS3ViaHdfsInterning(
      outputS3Path,
      hdfsTempPath => {
        data.write.option("header", writeHeader.toString).csv(hdfsTempPath)
      },
      copyType,
      tempDirectory)
  }

  private def writeAsSingleFile(data: DataFrame,
                                s3Path: String,
                                writeHeader: Boolean,
                                delimiter: String,
                                sortColumns: Seq[String])(implicit spark: SparkSession) : Unit = {

    val columnNames = data.columns
    var dataToSave = data.collect

    def getIntOrMin(row: Row, colIndex: Int) = {
      if(row.isNullAt(colIndex)) Int.MinValue else row.getInt(colIndex)
    }

    def getLongOrMin(row: Row, colIndex: Int) = {
      if(row.isNullAt(colIndex)) Long.MinValue else row.getLong(colIndex)
    }

    def getFloatOrMin(row: Row, colIndex: Int) = {
      if(row.isNullAt(colIndex)) Float.MinValue else row.getFloat(colIndex)
    }

    def getDoubleOrMin(row: Row, colIndex: Int) = {
      if(row.isNullAt(colIndex)) Double.MinValue else row.getDouble(colIndex)
    }

    for (sortColName <- sortColumns.reverse) {
      val sortColIndex = columnNames.indexOf(sortColName)
      if(sortColIndex != -1)
        data.schema(sortColIndex).dataType.typeName match {
          case "int" => dataToSave = dataToSave.sortBy(getIntOrMin(_, sortColIndex))
          case "integer" => dataToSave = dataToSave.sortBy(getIntOrMin(_, sortColIndex))
          case "long" => dataToSave = dataToSave.sortBy(getLongOrMin(_, sortColIndex))
          case "float" => dataToSave = dataToSave.sortBy(getFloatOrMin(_, sortColIndex))
          case "double" => dataToSave = dataToSave.sortBy(getDoubleOrMin(_, sortColIndex))
          case "string" => dataToSave = dataToSave.sortBy(_.getString(sortColIndex))
          case _ => throw new Exception("Unsupported sort column type: " + data.schema(sortColIndex).dataType.typeName)
        }
    }

    val builder = new StringBuilder()
    val newLine = System.getProperty("line.separator")

    // write the header, if requested
    if (writeHeader) {
      builder.append(columnNames.mkString(delimiter)).append(newLine)
    }

    // write the rows
    for (row <- dataToSave) {
      builder.append(row.mkString(delimiter)).append(newLine)
    }

    FSUtils.writeStringToFile(s3Path, builder.mkString)
  }

  def writeToS3ViaHdfsInterning(outputS3Path: String,
                                dataWriterFunction: String => Unit,
                                copyType: CopyType = OverwriteExisting,
                                tempDirectory: String = "hdfs:///user/hadoop/output-temp-dir",
                                tempFolderName: String = "unknown",
                                throwIfSourceEmpty: Boolean = false) (implicit spark: SparkSession) : List[String] = {
    // handle existing output case
    if (FSUtils.directoryExists(outputS3Path) && FSUtils.listDirectoryContents(outputS3Path).length > 0) {
      copyType match {
        case CopyToEmpty => throw new IOException("Non-empty output directory already exists!")
        case OverwriteExisting => FSUtils.cleanDirectory(outputS3Path) // delete ALL data in the root directory
        case AppendFoldersToExisting =>
        // Do nothing (for now). SparkDistCp deletes data in any folder it would write to, so any
        // folders we don't write to will stick around, while folders we write to will get overwritten.
      }
    }

    // make sure to create a new temporary folder that doesn't exist
    var hdfsTempPath: String = null
    do {
      hdfsTempPath = FSUtils.combinePaths(tempDirectory, s"$tempFolderName-${UUID.randomUUID().toString}")
    } while (FSUtils.directoryExists(hdfsTempPath))

    log.info(s"Starting writing to temp HDFS: $hdfsTempPath")
    // perform the actual writing to the temporary hdfs path
    dataWriterFunction(hdfsTempPath)
    log.info(s"Completed writing to temp HDFS: $hdfsTempPath")

    // copy from hdfs to s3
    log.info(s"Starting writing to S3: $outputS3Path")
    val destFiles = SparkDistCp.run(SparkDistCpArgs(hdfsTempPath, outputS3Path, cleanDestDir = true, throwIfSourceEmpty = throwIfSourceEmpty))
    log.info(s"Completed writing to S3: $outputS3Path")

    // delete the temporary hdfs directory
    log.info(s"Starting deleting temp HDFS: $hdfsTempPath")
    FSUtils.deleteDirectory(hdfsTempPath, recursive = true)

    log.info(s"Completed deleting temp HDFS: $hdfsTempPath")
    destFiles
  }
}

