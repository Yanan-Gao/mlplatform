package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.ttdEnv
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

abstract class KongMingDataset[T <: Product : Manifest](
  s3DatasetPath: String,
  defaultNumPartitions: Int
) {
  val MLPlatformS3Root: String = "thetradedesk-mlplatform-us-east-1/data"

  lazy val S3BasePath: String = s"s3a://${MLPlatformS3Root}/${ttdEnv}/kongming/${s3DatasetPath}"
  lazy val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def S3DatePartitionedPath(date: LocalDate): String = s"$S3BasePath/date=${date.format(dateFormatter)}"


  def writePartition(dataset: Dataset[T],
                     date: LocalDate,
                     numPartitions: Option[Int] = None): Unit = {

    val partitionedS3Path = S3DatePartitionedPath(date)

    dataset
      .repartition(numPartitions.getOrElse(defaultNumPartitions))
      .write.mode(SaveMode.Overwrite)
      .parquet(partitionedS3Path)
  }
}
