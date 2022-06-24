package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.ttdEnv
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class KongMingDataset[T <: Product : Manifest](
  s3DatasetPath: String,
  defaultNumPartitions: Int
) {
  val MLPlatformS3Root: String = "thetradedesk-mlplatform-us-east-1/data"

  lazy val S3BasePath: String = s"s3a://${MLPlatformS3Root}/${ttdEnv}/kongming/${s3DatasetPath}"

  lazy val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def S3DatePartitionedPath(
                             date: LocalDate,
                             subFolderKey: Option[String] = None,
                             subFolderValue: Option[String] = None): String = {
    subFolderKey match {
      case Some(subFolderKey) => s"$S3BasePath/date=${date.format(dateFormatter)}/$subFolderKey=${subFolderValue.getOrElse("")}"
      case _ => s"$S3BasePath/date=${date.format(dateFormatter)}"
    }

  }

  def writePartition(dataset: Dataset[T],
                     date: LocalDate,
                     numPartitions: Option[Int] = None,
                     subFolderKey: Option[String] = None,
                     subFolderValue: Option[String] = None,
                     format: Option[String] = None
                    ): Unit = {

    val partitionedS3Path = S3DatePartitionedPath(date, subFolderKey, subFolderValue)

    format match {

      case Some("tfrecord") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(SaveMode.Overwrite)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(partitionedS3Path)

      case  Some("csv") => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode("overwrite")
        .option("header",true)
        .csv(partitionedS3Path)

      case _ => dataset
        .repartition(numPartitions.getOrElse(defaultNumPartitions))
        .write.mode(SaveMode.Overwrite)
        .parquet(partitionedS3Path)
    }
  }
}