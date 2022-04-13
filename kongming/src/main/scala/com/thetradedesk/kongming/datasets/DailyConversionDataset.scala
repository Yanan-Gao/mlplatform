package com.thetradedesk.kongming.datasets

import org.apache.spark.sql.{Dataset, SaveMode}
import java.time.LocalDate

final case class DailyConversionDataRecord( TrackingTagId: String,
                                            UIID: String,
                                            DataAggKey: String,
                                            DataAggValue: String,
                                            ConversionTime: java.sql.Timestamp
                                          )

object DailyConversionDataset {
  val S3Path: String =
    s"s3a://thetradedesk-mlplatform-us-east-1/data/prod/kongming/dailyconversion/v=1/"

  val DEFAULT_NUM_PARTITION = 100

  def writePartition(dataset: Dataset[DailyConversionDataRecord],
                     date: LocalDate,
                     numPartitions: Option[Int] = None): Unit = {

    val partitionedS3Path = S3Path+"date="+date.toString

    dataset
      .repartition(numPartitions.getOrElse(DEFAULT_NUM_PARTITION))
      .write.mode(SaveMode.Overwrite)
      .parquet(partitionedS3Path)
  }

}
