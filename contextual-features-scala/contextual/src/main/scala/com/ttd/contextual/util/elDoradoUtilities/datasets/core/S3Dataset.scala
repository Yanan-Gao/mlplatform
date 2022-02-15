package com.ttd.contextual.util.elDoradoUtilities.datasets.core

import com.ttd.contextual.util.elDoradoUtilities.distcp.{AppendFoldersToExisting, DataOutputUtils}
import com.ttd.contextual.util.elDoradoUtilities.logging.Logger
import com.ttd.contextual.util.elDoradoUtilities.spark.{Local, LocalORC, LocalParquet, ProdTesting, Production, Sandbox, TTDConfig, Testing}
import com.ttd.contextual.util.elDoradoUtilities.logging.Logger
import com.ttd.contextual.spark.TTDSparkContext.spark
import com.ttd.contextual.spark.TTDSparkContext.spark.implicits._
import com.ttd.contextual.util.elDoradoUtilities.datasets.core.DataSetMode.DataSetMode
import com.ttd.contextual.util.elDoradoUtilities.spark.listener.WriteListener
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig.config
import com.ttd.contextual.util.elDoradoUtilities.spark.TTDConfig
import com.ttd.contextual.util.elDoradoUtilities.spark._
import com.ttd.contextual.util.elDoradoUtilities.distcp.{AppendFoldersToExisting, DataOutputUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.util.Try


sealed trait DataSetType

/**
 * Source datasets are always drawn from prod sources, and are usually not written to.
 */
case object SourceDataSet extends DataSetType

/**
 * Generated Datasets are given special treatment in sandbox mode, where they are written to and read from in the users personal sandbox,
 * and are typically written in a human-readable format for easy debugging.
 */
case object GeneratedDataSet extends DataSetType


/**
 * An abstraction of a set of data stored on S3. Contains helper methods for writing and reading data, as well as auto-formatting
 *
 * @param dataSetType    Whether this is a Source dataset or a Generated Dataset. Source datasets are always drawn from prod sources, and are usually not written to.
 *                       Generated Datasets are given special treatment in sandbox mode, where they are written to and read from in the users personal sandbox, and
 *                       are typically written in a human-readable format for easy debugging.
 * @param rootFolderPath a relative path from the root folder to where this dataset is stored.
 * @param manifest       the manifest for the case class provided. implicit as part of any case class
 * @tparam T A case class containing the names and data types of all columns in this data set
 */
abstract class S3DataSet[T <: Product](
                                        dataSetType: DataSetType,
                                        s3RootPath: String,
                                        val rootFolderPath: String,
                                        fileFormat: FileFormat = Parquet,
                                        mergeSchema: Boolean = false
                                      )(implicit val manifest: Manifest[T]) extends Serializable with Logger {
  /**
   * If you need to change any logic here, remember to also make sure that PartitionedS3DataSet also gets changes, to maintain logical parity
   */
  val dataSetName: String = this.getClass.getSimpleName
  val s3TestPath: String = config.getString("ttd.global.test.s3path", "test") // to have a test location separate from `/test`
  protected val s3Root: String = normalizeUri(config.getString("ttd.s3root", s3RootPath))
  protected val s3RootProd: String = normalizeUri(config.getString(s"ttd.$dataSetName.prod.s3root", s"${s3Root}prod"))
  protected val s3RootTest: String = normalizeUri(config.getString(s"ttd.$dataSetName.test.s3root", s"$s3Root$s3TestPath"))
  protected val s3RootSandbox: String = normalizeUri(
    config.getString(s"ttd.$dataSetName.sandbox.s3root", s"$s3Root/sandbox/${TTDConfig.user}")
  )
  protected val localRoot: String = normalizeUri(config.getString(s"ttd.$dataSetName.sandbox.s3root", s"local-data"))
  protected val schema: StructType = implicitly[Encoder[T]].schema

  lazy val (readRoot, readFormat, writeRoot, writeFormat) = TTDConfig.environment match {
    case Production =>
      (s3RootProd, fileFormat(DataSetMode.in, fileFormat), s3RootProd, fileFormat(DataSetMode.out, fileFormat))
    case ProdTesting =>
      (s3RootProd, fileFormat(DataSetMode.in, fileFormat), s3RootTest, fileFormat(DataSetMode.out, fileFormat))
    case Testing if dataSetType == SourceDataSet =>
      (s3RootProd, fileFormat(DataSetMode.in, fileFormat), s3RootTest, fileFormat(DataSetMode.out, fileFormat))
    case Testing if dataSetType == GeneratedDataSet =>
      (s3RootTest, fileFormat(DataSetMode.in, fileFormat), s3RootTest, fileFormat(DataSetMode.out, fileFormat))
    case Sandbox if dataSetType == SourceDataSet =>
      (s3RootProd, fileFormat(DataSetMode.in, fileFormat), s3RootSandbox, fileFormat(DataSetMode.out, Csv.WithHeader))
    case Sandbox if dataSetType == GeneratedDataSet =>
      (s3RootSandbox, fileFormat(DataSetMode.in, Csv.WithHeader), s3RootSandbox, fileFormat(DataSetMode.out, Csv.WithHeader))
    case Local =>
      (localRoot, fileFormat(DataSetMode.in, Csv.WithHeader), localRoot, fileFormat(DataSetMode.out, Csv.WithHeader))
    case LocalParquet =>
      (localRoot, fileFormat(DataSetMode.in, Parquet), localRoot, fileFormat(DataSetMode.out, Parquet))
    case LocalORC =>
      (localRoot, fileFormat(DataSetMode.in, ORC), localRoot, fileFormat(DataSetMode.out, ORC))
  }

  /**
   * @return This dataset as a raw dataframe. Try to work in datasets whenever possible so that we can type-check at compile time
   */
  protected def readDataFrame(rootFolderPaths: String*): DataFrame = read(rootFolderPaths: _*).toDF

  protected def normalizeUri(uri: String): String = {
    if (!uri.endsWith("/"))
      uri + "/"
    else
      uri
  }

  private def fileFormat(mode: DataSetMode, defaultFormat: FileFormat): FileFormat = {
    config.getStringOption(s"ttd.$dataSetName.${mode}Format") match {
      case Some("parquet") => Parquet
      case Some("json") => Json
      case Some("csv") => config.getStringOption(s"ttd.$dataSetName.${mode}FormatProps.Header") match {
        case Some("false") => Csv.Headerless
        case _ => Csv.WithHeader
      }
      case Some("tsv") =>
        config.getStringOption(s"ttd.$dataSetName.${mode}FormatProps.Header") match {
          case Some("false") => Tsv.Headerless
          case _ => Tsv.WithHeader
        }
      case _ => defaultFormat
    }
  }

  /**
   * Read from this dataset. This reads from different locations based on the value of the ttd.env flag:
   * prod: read from production on s3.
   * sandbox: If this is a Source dataset, read from production. If this is a Generated dataset, read from the users sandbox directory on s3
   * local: Read from the local-data folder in this project
   *
   * @param rootFolderPaths Optional parameter with arbitrary list of path to read from.
   *                        If this parameter is empty the path provided in the constructor param `rootFolderPath` will be used
   *                        (default behavior).
   * @return A DataSet[T]
   */
  protected def read(rootFolderPaths: String*): Dataset[T] = {
    val paths = if (rootFolderPaths.nonEmpty) rootFolderPaths.map(path => s"$readRoot$path")
    else Seq(s"$readRoot$rootFolderPath")

    log.info(s"Reading from ${paths.mkString("'", ", ", "'")} as $readFormat")
    // set mergeSchema to true if the dataset schema is evolving, otherwise .as[] will throw an exception
    val reader = spark.read
      .option("basePath", readRoot)
      .option("mergeSchema", mergeSchema)
    val dataFrame = readFormat match {
      case Parquet =>
        // Parquet files read from schema can have problems with decimal precision we don't really care about,
        // so rely on the .as cast for parquet files
        reader.parquet(paths: _*)
      case Json =>
        // JSON files have problems when an optional value is not included in the written object
        // So use the schema to tell spark "Yes, we promise it's supposed to be there"
        reader.schema(schema).json(paths: _*)
      case Csv(withHeader) =>
        // Ditto for CSV files.
        // el-dorado requires that all read csv files have a header, so we can match columns to datatypes.
        reader.schema(schema).option("header", withHeader.toString).csv(paths: _*)
      case Tsv(withHeader) =>
        reader.schema(schema).options(Map("sep" -> "\t", "header" -> withHeader.toString)).csv(paths: _*)
      case ORC =>
        // orc files are schema-less, so spark will assume the columns are in the same order as the specified schema
        // the schema is generated from the case class of the data set
        reader.schema(schema).orc(paths: _*)
    }
    dataFrame.as[T]
  }

  /**
   * Write to this dataset Normally only allowed for Generated datasets This writes to different locations based on the value of the ttd.env flag:
   * prod: write to production on s3
   * sandbox: write to the users sandbox on s3
   * local: write to the local-data folder in this project
   *
   * @param dataSet             the dataset[T] to write to this folder. Overwrites any data that already exists in this folder.
   * @param overrideSourceWrite If this is set to true, write to Source datasets. Should only ever be set for initial ETL jobs
   */
  protected def write(dataSet: Dataset[T], coalesceToNumFiles: Option[Int] = None, overrideSourceWrite: Boolean = false, tempDirectory: String): Long = {
    val path = s"$writeRoot$rootFolderPath"
    log.info(s"Writing to $path as $writeFormat with $coalesceToNumFiles buckets")
    if (!overrideSourceWrite && dataSetType == SourceDataSet) throw new IllegalAccessException(
      s"May not write data to source data set $dataSetName"
    )
    val partitionedDataset = coalesceToNumFiles match {
      case Some(partNum) => dataSet.repartition(partNum)
      case None => dataSet
    }

    def writer(dataset: Dataset[T]) = dataset.write.mode(SaveMode.Overwrite)

    val dataWriterFunction: String => Unit = writeFormat match {
      case Parquet =>
        pathstr => writer(partitionedDataset).parquet(pathstr)
      case Json =>
        pathstr => writer(partitionedDataset).json(pathstr)
      case Csv(_) =>
        // For the sake of encouraging/enforcing best-practice data writes we always write headers for CSV/TSV file formats
        pathstr => writer(partitionedDataset).option("header", "true").csv(pathstr)
      case Tsv(_) =>
        // For the sake of encouraging/enforcing best-practice data writes we always write headers for CSV/TSV file formats
        pathstr => writer(partitionedDataset).options(Map("sep" -> "\t", "header" -> "true")).csv(pathstr)
      case ORC =>
        pathstr => writer(partitionedDataset).orc(pathstr)
    }

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    val result = Try {
      if (TTDConfig.environment == Local || TTDConfig.environment == LocalParquet
      || tempDirectory.equals("")) {
        dataWriterFunction(path) // Just write the data
      } else {
        // If we're on EMR, write to HDFS then copy to Spark when completed.
        DataOutputUtils.writeToS3ViaHdfsInterning(
          path,
          dataWriterFunction,
          AppendFoldersToExisting,
          tempFolderName = dataSetName,
          tempDirectory = tempDirectory
        )(spark)
      }

      listener.rowsWritten
    }

    // remove listener, get stats
    spark.sparkContext.removeSparkListener(listener)

    result.get
  }
}

