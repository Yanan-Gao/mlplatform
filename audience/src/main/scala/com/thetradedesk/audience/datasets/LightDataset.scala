package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly, parquetDataPaths, parquetHourlyDataPaths}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.thetradedesk.featurestore.data.cbuffer.SchemaHelper.{CBufferDataFrameReader, CBufferDataFrameWriter, indicateArrayLength}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

object DatasetSource {
  val Logs: String = "Log"
  val CrossDeviceGraph: String = "CrossDeviceGraph"
}

abstract class LightDataset(dataSetPath: String,
                            rootPath: String,
                            dateFormat: String) {
  lazy val basePath: String = LightDataset.ConcatPath(dataSetPath, rootPath)
  lazy val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
  lazy val logsDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  lazy val crossDeviceDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def DatePartitionedPath(
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

object LightDataset {
  private def ConcatPath(dataSetPath: String,
                         rootPath: String): String = {
    TrimPath(rootPath) + "/" + TrimPath(dataSetPath)
  }

  // remove slash letter with head and tail
  private def TrimPath(path: String): String = {
    path.stripPrefix("/").stripSuffix("/")
  }
}

abstract class LightWritableDataset[T <: Product : Manifest](
                                                              dataSetPath: String,
                                                              rootPath: String,
                                                              defaultNumPartitions: Int,
                                                              dateFormat: String = "yyyyMMdd"
                                                            ) extends LightDataset(dataSetPath, rootPath, dateFormat) {

  def writePartition(dataset: Dataset[T],
                     partition: Any = null,
                     numPartitions: Option[Int] = None,
                     subFolderKey: Option[String] = None,
                     subFolderValue: Option[String] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.ErrorIfExists,
                     maxChunkRecordCount: Int = 20480
                    ): Unit = {

    val partitionedPath: String = DatePartitionedPath(Option.apply(partition), subFolderKey, subFolderValue)

    format match {

      case Some("tfrecord") => dataset
        .coalesce(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(partitionedPath)

      case Some("csv") => dataset
        .coalesce(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .option("header", value = true)
        .csv(partitionedPath)

      case Some("parquet") => dataset
        .coalesce(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .parquet(partitionedPath)
      
      case Some("cbuffer") => dataset
        .coalesce(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        // determine how many records in one chunk, when reading in python, batch size needs to be larger than this setting
        .option("maxChunkRecordCount", maxChunkRecordCount)
        .cb(partitionedPath)

      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }
}

abstract class LightReadableDataset[T <: Product : Manifest](
                                                              dataSetPath: String,
                                                              rootPath: String,
                                                              dateFormat: String = "yyyyMMdd",
                                                              source: Option[String] = None
                                                            ) extends LightDataset(dataSetPath, rootPath, dateFormat) {
  private def subFolderPath(date: LocalDate,
                            lookBack: Option[Int] = None,
                            dateSeparator: Option[String] = None,
                            subFolderKey: Option[String] = None,
                            subFolderValue: Option[Any] = None)(implicit spark: SparkSession) : Seq[String] = {
    (source match {
      case Some(DatasetSource.Logs) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(logsDateFormatter)}")
      case Some(DatasetSource.CrossDeviceGraph) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(crossDeviceDateFormatter)}/success")
      case _ => {
        (subFolderValue match {
          case Some(stringFolderValue: String) =>
            val subFolder = if (subFolderKey.nonEmpty && subFolderValue.nonEmpty) s"/${subFolderKey.get}=${stringFolderValue}" else ""
            parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator).map(e => s"$e$subFolder")
          case Some(iterable: Iterable[Any]) =>
            for {x <- parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator)
                 y <- iterable.map(e => s"/${subFolderKey.get}=$e")}
            yield s"$x$y"
          case _ => parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator)
        })
      }
    }).filter(FSUtils.directoryExists(_)(spark))
  }

  def readPartition(date: LocalDate,
                    format: Option[String] = Some("parquet"),
                    lookBack: Option[Int] = None,
                    dateSeparator: Option[String] = None,
                    subFolderKey: Option[String] = None,
                    subFolderValue: Option[Any] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = subFolderPath(date, lookBack, dateSeparator, subFolderKey, subFolderValue)

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
      case Some("cbuffer") => spark.read.cb(paths: _*).selectAs[T]
  
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
      case Some("cbuffer") => spark.read.cb(paths: _*).selectAs[T]
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
      case Some("cbuffer") => spark
        .read
        .cb(path)
        .selectAs[T]
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }
}