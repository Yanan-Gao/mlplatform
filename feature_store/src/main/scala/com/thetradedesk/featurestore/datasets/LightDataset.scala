package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore.utils.PathUtils
import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly, parquetDataPaths, parquetHourlyDataPaths}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.distcp.{AppendFoldersToExisting, CopyToEmpty, DataOutputUtils, OverwriteExisting}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

object DatasetSource {
  val Logs: String = "Log"
  val CrossDeviceGraph: String = "CrossDeviceGraph"
}

trait LightDataset {
  val dataSetPath: String
  val rootPath: String
  val dateFormat: String = "yyyyMMdd"
  lazy val basePath: String = PathUtils.concatPath(rootPath, dataSetPath)
  lazy val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
  lazy val logsDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  lazy val crossDeviceDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def datePartitionedPath(
                           partition: Option[Any] = None,
                           subFolderKey: Option[String] = None,
                           subFolderValue: Option[String] = None): String = {
    partition match {
      case Some(localDate: LocalDate) => subFolderKey match {
        case Some(subFolderKey) => s"$basePath/date=${localDate.format(dateFormatter)}/$subFolderKey=${subFolderValue.getOrElse("")}"
        case _ => s"$basePath/date=${localDate.format(dateFormatter)}"
      }
      case Some(localDateTime: LocalDateTime) => subFolderKey match {
        case Some(subFolderKey) => s"$basePath/${localDateTime.format(dateFormatter)}/$subFolderKey=${subFolderValue.getOrElse("")}"
        case _ => s"$basePath/${localDateTime.format(dateFormatter)}"
      }
      case Some(value: String) => subFolderKey match {
        case Some(subFolderKey) => s"$basePath/$value/$subFolderKey=${subFolderValue.getOrElse("")}"
        case _ => s"$basePath/$value"
      }
      case _ => subFolderKey match {
        case Some(subFolderKey) => s"$basePath/$subFolderKey=${subFolderValue.getOrElse("")}"
        case _ => s"$basePath"
      }
    }
  }
}

trait LightWritableDataset[T <: Product] extends LightDataset {
  val defaultNumPartitions: Int
  val repartitionColumn: Option[String] = None
  val maxRecordsPerFile: Int = 0
  val writeThroughHdfs: Boolean = false

  def writePartition(dataset: Dataset[T],
                     partition: Any = null,
                     numPartitions: Option[Int] = None,
                     subFolderKey: Option[String] = None,
                     subFolderValue: Option[String] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.ErrorIfExists
                    ): Unit = {

    val partitionedPath: String = datePartitionedPath(Option.apply(partition), subFolderKey, subFolderValue)
    val df = if (repartitionColumn.isEmpty) {
      dataset.coalesce(numPartitions.getOrElse(defaultNumPartitions))
    }
    else if (repartitionColumn.get.isEmpty) {
      dataset.repartition(numPartitions.getOrElse(defaultNumPartitions))
    } else {
      dataset.repartition(numPartitions.getOrElse(defaultNumPartitions), col(repartitionColumn.get))
    }

    format match {

      case Some("tfrecord") => df
        .write
        .mode(saveMode)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .option("maxRecordsPerFile", maxRecordsPerFile)
        .save(partitionedPath)

      case Some("csv") => df
        .write
        .mode(saveMode)
        .option("header", value = true)
        .option("maxRecordsPerFile", maxRecordsPerFile)
        .csv(partitionedPath)

      case Some("parquet") => {
        // write to hdfs first then copy to s3
        if (writeThroughHdfs) {
          val copyType = saveMode match {
            case SaveMode.Overwrite => OverwriteExisting
            case SaveMode.ErrorIfExists => CopyToEmpty
            case _ => AppendFoldersToExisting
          }

          DataOutputUtils.writeParquetThroughHdfs(df.toDF(), partitionedPath, copyType)(spark)
        } else {
          df
            .write
            .mode(saveMode)
            .option("maxRecordsPerFile", maxRecordsPerFile)
            .parquet(partitionedPath)
        }
      }
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }
}

trait LightReadableDataset[T <: Product] extends LightDataset {
  val source: Option[String] = None
  implicit val enc: Encoder[T]
  implicit val tt: TypeTag[T]

  def readPartition(date: LocalDate,
                    format: Option[String] = Some("parquet"),
                    lookBack: Option[Int] = None,
                    dateSeparator: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = (source match {
      case Some(DatasetSource.Logs) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(logsDateFormatter)}")
      case Some(DatasetSource.CrossDeviceGraph) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(crossDeviceDateFormatter)}/success")
      case _ => parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator)
    }).filter(FSUtils.directoryExists(_)(spark))

    format match {
      case Some("tfrecord") => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(paths: _*)
        .selectAs[T]
      case Some("tsv") => spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(paths.flatMap(x => FSUtils.listFiles(x, true)(spark).map(y => x + "/" + y)): _*)
        .toDF(typeOf[T].members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }: _*)
        .selectAs[T]
      case Some("parquet") => {
        source match {
          case Some(DatasetSource.CrossDeviceGraph) => spark.read.parquet(paths: _*).selectAs[T]
          case _ => loadParquetData[T](basePath, date, source, lookBack, dateSeparator)
        }
      }
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }

  def readPartitionHourly(date: LocalDate,
                          hours: Seq[Int],
                          format: Option[String] = Some("parquet"))(implicit spark: SparkSession): Dataset[T] = {
    val paths = source match {
      case Some(DatasetSource.Logs) => hours.map(h => "%s/%s/%02d".format(basePath, date.format(logsDateFormatter), h))
      case _ => parquetHourlyDataPaths(basePath, date, source, hours)
    }

    format match {
      case Some("tfrecord") => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(paths: _*)
        .selectAs[T]
      case Some("tsv") => spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .load(paths.flatMap(x => FSUtils.listFiles(x, true)(spark).map(y => x + "/" + y)): _*)
        .selectAs[T]
      case Some("parquet") => loadParquetDataHourly[T](basePath, date, hours, source)
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }

  def readSinglePartition(date: LocalDateTime,
                          format: Option[String] = Some("parquet"))(implicit spark: SparkSession): Dataset[T] = {
    val path = s"$basePath/${date.format(dateFormatter)}"

    format match {
      case Some("tfrecord") => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(path)
        .selectAs[T]
      case Some("tsv") => spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
        .toDF(typeOf[T].members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }: _*)
        .selectAs[T]
      case Some("parquet") => spark
        .read
        .parquet(path)
        .selectAs[T]
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }
}