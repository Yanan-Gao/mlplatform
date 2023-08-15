package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.io.FSUtils


import org.apache.spark.sql.Dataset

import java.time.LocalDate
import java.time.format.DateTimeFormatter

final case class OnlineLogsRecord(
                                   Timestamp: String,
                                   AvailableBidRequestId: String,
                                   Features: String,
                                   OnlineModelScore: Option[String],
                                   ModelVersion: String,
                                   FeaturesVersion: String,
                                   DynamicFeatures: String
                                 )

object OnlineLogsDataset {
  val S3Path: String = "s3://thetradedesk-useast-logs-2/valuealgofeaturediscrepancylogger/collected"

  def readOnlineLogsDataset(date: LocalDate, modelName: String): Dataset[OnlineLogsRecord] = {
    val dateStr = date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
    val dir = s"${S3Path}/${dateStr}"
    val filePaths = FSUtils.listFiles(dir, recursive = true)(spark).filter(x => x.contains(modelName.toLowerCase())).map(y => dir + "/" + y)

    val logs = spark.read.format("com.databricks.spark.csv")
      .option("sep", "\t")
      .option("header", "false")
      .load(filePaths: _*)
      .toDF("Timestamp", "AvailableBidRequestId", "Features", "OnlineModelScore", "ModelVersion", "FeaturesVersion", "DynamicFeatures")

    logs.selectAs[OnlineLogsRecord]
  }
}