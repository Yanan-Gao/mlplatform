package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly, parquetDataPaths, parquetHourlyDataPaths}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class LightDataset(dataSetPath: String,
                            rootPath: String,
                            dateFormat: String) {
  lazy val basePath: String = LightDataset.ConcatPath(dataSetPath, rootPath)
  private lazy val dateFormatter = DateTimeFormatter.ofPattern(dateFormat)

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
    format match {
      case Some("tfrecord") => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(parquetDataPaths(basePath, date, source, lookBack, separator = dateSeparator) : _*)
        .selectAs[T]
      case _ => loadParquetData[T](basePath, date, source, lookBack, dateSeparator)
    }
  }

  def readPartitionHourly(date: LocalDate,
                          hours: Seq[Int],
                          format: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
    format match {
      case Some("tfrecord") => spark
        .read
        .format("tfrecord")
        .option("recordType", "Example")
        .load(parquetHourlyDataPaths(basePath, date, source, hours) : _*)
        .selectAs[T]
      case _ => loadParquetDataHourly[T](basePath, date, hours, source)
    }
  }
}