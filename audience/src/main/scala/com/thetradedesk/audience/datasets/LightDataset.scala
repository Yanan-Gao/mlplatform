package com.thetradedesk.audience.datasets

import com.thetradedesk.geronimo.shared.{loadParquetData, loadParquetDataHourly}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class LightDataset(dataSetPath: String,
                            rootPath: String,
                            dateFormat: String) {
  lazy val basePath: String = s"${rootPath}/${dataSetPath}"
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
                     format: Option[String] = None
                    ): Unit = {

    val partitionedPath: String = DatePartitionedPath(Some(date), subFolderKey, subFolderValue)

    format match {

      case Some("tfrecord") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(partitionedPath)

      case _ => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(SaveMode.Overwrite)
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
      case _ => loadParquetData[T](basePath, date, source, lookBack, dateSeparator)
    }
  }

  def readPartitionHourly(date: LocalDate,
                          hours: Seq[Int],
                          format: Option[String] = None)(implicit spark: SparkSession): Dataset[T] = {
    format match {
      case _ => loadParquetDataHourly[T](basePath, date, hours, source)
    }
  }
}