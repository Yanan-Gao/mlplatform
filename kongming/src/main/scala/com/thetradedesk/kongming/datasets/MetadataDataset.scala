package com.thetradedesk.kongming.datasets

import com.thetradedesk.MetadataType
import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core.{Csv, DatePartitionedS3DataSet, GeneratedDataSet}
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

/**
 * This class is for the meta-data metrics, e.g. row counts and job runtime
 */
final case class MetadataSchema(
                               MetadataType: String,
                               MetadataName: String,
                               MetadataValue: Long,
                               Timestamp: java.sql.Timestamp,
                               )

case class MetadataDataset(experimentOverride: Option[String] = None) extends DatePartitionedS3DataSet[MetadataSchema](
  dataSetType = GeneratedDataSet,
  s3RootPath = MLPlatformS3Root,
  rootFolderPath = s"${BaseFolderPath}/meta_data",
  fileFormat = Csv.WithHeader,
  partitionField = "date",
  writeThroughHdfs = false,
  experimentOverride = experimentOverride
) {
  /**
   * Save metadata for each run. It serves as changelog, given the timestamp is also recorded
   * and the data is written in append mode
   */
  def writeRecord[T](metricData: Long, partition: LocalDate, metadataType: MetadataType.Value, metadataName: String): Unit = {

    val dateTime = Timestamp.valueOf(LocalDateTime.now())

    val rdd = spark.sparkContext.parallelize(Seq(MetadataSchema(
      metadataType.toString,
      metadataName,
      metricData,
      dateTime
    )))

    val df = spark.createDataFrame(rdd).selectAs[MetadataSchema]

    super.writePartition(df, partition, Some(1))

  }
}
