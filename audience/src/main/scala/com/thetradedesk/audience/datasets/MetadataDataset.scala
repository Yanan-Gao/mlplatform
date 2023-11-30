package com.thetradedesk.audience.datasets

import com.thetradedesk.audience.{audienceVersionDateFormat, ttdEnv}
import com.thetradedesk.audience.datasets.S3Roots.ML_PLATFORM_ROOT
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.SaveMode

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

/**
 * This class is for the meta-data metrics, e.g. size_count and job runtime
 */




final case class MetadataSchema(
                                 MetadataType: String,
                                 MetricName: String,
                                 MetadataValue: Double,
                                 Timestamp: java.sql.Timestamp,
                               )

case class MetadataDataset(version: Int = 1) extends LightWritableDataset[MetadataSchema](
  s"/${ttdEnv}/audience/RSM/metadata/v=${version.toString}", S3Roots.ML_PLATFORM_ROOT, 1)
{
  /**
   * Save metadata for each run. It serves as changelog, given the timestamp is also recorded
   * and the data is written in append mode
   */
  def writeRecord[T](metricData: Double, partition: LocalDateTime, metadataType: String, metricName: String): Unit = {

    val dateTime = Timestamp.valueOf(LocalDateTime.now())

    val rdd = spark.sparkContext.parallelize(Seq(MetadataSchema(
      metadataType,
      metricName,
      metricData,
      dateTime
    )))

    val df = spark.createDataFrame(rdd).selectAs[MetadataSchema]

    super.writePartition(df, partition, Some(1),format = Some("csv"),saveMode = SaveMode.Append)
  }
}

