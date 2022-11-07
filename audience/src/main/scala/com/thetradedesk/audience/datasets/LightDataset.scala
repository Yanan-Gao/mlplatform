package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly, parquetDataPaths, parquetHourlyDataPaths}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.io.FSUtils

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.reflect.runtime.universe._

object DatasetSource {
  val Logs: String = "Log"
}

abstract class LightDataset(dataSetPath: String,
                            rootPath: String,
                            dateFormat: String) {
  lazy val basePath: String = LightDataset.ConcatPath(dataSetPath, rootPath)
  private lazy val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)
  lazy val logsDateFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")

  def DatePartitionedPath(
                           date: Option[LocalDate] = None,
                           subFolderKey: Option[String] = None,
                           subFolderValue: Option[String] = None): String = {
    date match {
      case Some(date) => subFolderKey match {
        case Some(subFolderKey) => s"$basePath/date=${date.format(dateFormatter)}/$subFolderKey=${subFolderValue.getOrElse("")}"
        case _ => s"$basePath/date=${date.format(dateFormatter)}"
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
    path match {
      case x if x.startsWith("/") && x.endsWith("/") => x.substring(1, x.length - 1)
      case x if x.endsWith("/") => x.substring(0, x.length - 1)
      case x if x.startsWith("/") => x.substring(1, x.length)
      case _ => path
    }
  }
}

abstract class LightWritableDataset[T <: Product : Manifest](
                                                              dataSetPath: String,
                                                              rootPath: String,
                                                              defaultNumPartitions: Int,
                                                              dateFormat: String = "yyyyMMdd"
                                                            ) extends LightDataset(dataSetPath, rootPath, dateFormat) {


  def writePartition(dataset: Dataset[T],
                     date: LocalDate,
                     numPartitions: Option[Int] = None,
                     subFolderKey: Option[String] = None,
                     subFolderValue: Option[String] = None,
                     format: Option[String] = None,
                     saveMode: SaveMode = SaveMode.ErrorIfExists
                    ): Unit = {

    val partitionedPath: String = DatePartitionedPath(Some(date), subFolderKey, subFolderValue)

    format match {

      case Some("tfrecord") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(partitionedPath)

      case  Some("csv") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .option("header", value = true)
        .csv(partitionedPath)

      case _ => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .parquet(partitionedPath)
    }
  }
}

abstract class LightReadableDataset[T <: Product : Manifest](
                                                              dataSetPath: String,
                                                              rootPath: String,
                                                              dateFormat: String = "yyyyMMdd",
                                                              source: Option[String] = None
                                                            ) extends LightDataset(dataSetPath, rootPath, dateFormat) {
  def readPartition(date: LocalDate,
                    format: Option[String] = None,
                    lookBack: Option[Int] = None,
                    dateSeparator: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
    val paths = source match {
      case Some(DatasetSource.Logs) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(logsDateFormatter)}")
      case _ => parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator)
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
        .option("inferSchema", "true")
        .load(paths.flatMap(x => FSUtils.listFiles(x, true)(spark).map(y => x + "/" + y)): _*)
        .toDF(typeOf[T].members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }: _*)
        .selectAs[T]
      case _ => loadParquetData[T](basePath, date, source, lookBack, dateSeparator)
    }
  }

  def readPartitionHourly(date: LocalDate,
                          hours: Seq[Int],
                          format: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
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
      case _ => loadParquetDataHourly[T](basePath, date, hours, source)
    }
  }
}