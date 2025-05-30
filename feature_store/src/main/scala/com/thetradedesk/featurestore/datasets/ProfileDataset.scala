package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.utils.PathUtils
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}


case class ProfileDataset(dataSetPath: String = ProfileDataBasePath,
                          rootPath: String = MLPlatformS3Root,
                          sourcePartition: String,
                          indexPartition: String,
                          environment: String = ttdEnv) extends ProfileBaseDataset(
  dataSetPath,
  rootPath,
  environment
) {
  override val subPartitions: String = s"source=${sourcePartition}/index=${indexPartition}/v=1"

  def writeWithRowCountLog(dataset: Dataset[_], datePartition: Any = null, numPartitions: Option[Int] = None): (String, Long) = {
    val rows = super.writePartition(dataset, datePartition, numPartitions)
    val logName = s"source=${sourcePartition}/index=${indexPartition}"
    (logName, rows)
  }
}


abstract class ProfileBaseDataset(dataSetPath: String,
                                  rootPath: String,
                                  environment: String) {
  lazy val basePath: String = PathUtils.concatPath(rootPath, dataSetPath)
  lazy val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val subPartitions: String

  def datePartitionedPath(datePartition: Option[Any] = None): String = {
    datePartition match {
      case Some(localDate: LocalDate) => s"date=${localDate.format(dateFormatter)}"
      case Some(localDateTime: LocalDateTime) => localDateTime.format(dateFormatter)
      case Some(value: String) => value
      case _ => ""
    }
  }

  def writePartition(dataset: Dataset[_],
                     datePartition: Any = null,
                     numPartitions: Option[Int] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.Overwrite
                    ): Long = {

    val writeEnv = if (environment == "prodTest") "test" else environment

    val datePath: String = datePartitionedPath(Option.apply(datePartition))

    val writePath = Array(rootPath, writeEnv, dataSetPath, subPartitions, datePath).reduce(PathUtils.concatPath)

    format match {
      case Some("tfrecord") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(writePath)

      case Some("csv") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .option("header", value = true)
        .csv(writePath)

      case Some("parquet") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(saveMode)
        .parquet(writePath)
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }

    dataset.count()
  }
}
