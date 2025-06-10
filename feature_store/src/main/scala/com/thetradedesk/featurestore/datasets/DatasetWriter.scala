package com.thetradedesk.featurestore.datasets

import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.util.distcp.{AppendFoldersToExisting, CopyToEmpty, DataOutputUtils, OverwriteExisting}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode}

object DatasetWriter {

  def writeDataSet(dataset: Dataset[_],
                   writePath: String,
                   numPartitions: Option[Int] = None,
                   repartitionColumn: Option[String] = None,
                   format: Option[String] = Some("parquet"),
                   saveMode: SaveMode = SaveMode.Overwrite,
                   maxRecordsPerFile: Int = 0,
                   writeThroughHdfs: Boolean = false): Unit = {

    // partition the dataset
    var df = dataset
    if (!numPartitions.isEmpty) { // skip if numPartitions is None
      df = if (repartitionColumn.isEmpty) {
        // Case 1: repartitionColumn is None
        dataset.coalesce(numPartitions.get)
      }
      else if (repartitionColumn.get.isEmpty) {
        // Case 2: repartitionColumn is Some("")
        dataset.repartition(numPartitions.get)
      } else {
        // Case 3: repartitionColumn is Some("columnName")
        dataset.repartition(numPartitions.get, col(repartitionColumn.get))
      }
    }

    format match {

      case Some("tfrecord") => df
        .write
        .mode(saveMode)
        .format("tfrecord")
        .option("recordType", "Example")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .option("maxRecordsPerFile", maxRecordsPerFile)
        .save(writePath)

      case Some("csv") => df
        .write
        .mode(saveMode)
        .option("header", value = true)
        .option("maxRecordsPerFile", maxRecordsPerFile)
        .csv(writePath)

      case Some("parquet") => {
        // write to hdfs first then copy to s3
        if (writeThroughHdfs) {
          val copyType = saveMode match {
            case SaveMode.Overwrite => OverwriteExisting
            case SaveMode.ErrorIfExists => CopyToEmpty
            case _ => AppendFoldersToExisting
          }

          DataOutputUtils.writeParquetThroughHdfs(df.toDF(), writePath, copyType)(spark)
        } else {
          df
            .write
            .mode(saveMode)
            .option("maxRecordsPerFile", maxRecordsPerFile)
            .parquet(writePath)
        }
      }
      case _ => throw new UnsupportedOperationException(s"format ${format} is not supported")
    }
  }
}
