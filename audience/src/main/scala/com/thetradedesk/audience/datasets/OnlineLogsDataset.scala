package com.thetradedesk.audience.datasets

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import scala.reflect.runtime.universe._

final case class OnlineLogsRecord(
                                 Timestamp: String,
                                 AvailableBidRequestId: String,
                                 Features: String,
                                 OnlineModelScore: Option[String],
                                 ModelVersion: String,
                                 FeaturesVersion: String,
                                 DynamicFeatures: String
                                 )

case class OnlineLogsDataset(modelName: String,
                             dataSetPath: String = s"valuealgofeaturediscrepancylogger/collected/",
                             rootPath: String = "s3://thetradedesk-useast-logs-2/",
                             dateFormat: String = "yyyy/MM/dd",
                             source: Option[String] = Some(DatasetSource.Logs)) extends
  LightReadableDataset[OnlineLogsRecord](dataSetPath, rootPath, dateFormat, source) {
  override def readPartition(date: LocalDate,
                             format: Option[String] = Some("tsv"),
                             lookBack: Option[Int] = None,
                             dateSeparator: Option[String] = Some("/"))(implicit spark: SparkSession): Dataset[OnlineLogsRecord] = {
    val paths = (source match {
      case Some(DatasetSource.Logs) => (0 to lookBack.getOrElse(0)) map (x => s"$basePath/${date.minusDays(x).format(logsDateFormatter)}/")
      case _ => throw new NotImplementedError("Unsupported source for online logs")
    }).filter(FSUtils.directoryExists(_)(spark))

    format match {
      case Some("tsv") => spark
        .read
        .format("com.databricks.spark.csv")
        .option("sep", "\t")
        .option("header", "false")
        .load(paths.flatMap(x => FSUtils.listFiles(x, recursive = true)(spark).filter(x => x.contains(modelName.toLowerCase())).map(y => x + "/" + y)): _*)
        .toDF(typeOf[OnlineLogsRecord].members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }: _*)
        .selectAs[OnlineLogsRecord]
      case _ => throw new NotImplementedError("Unsupported format for online logs")
    }
  }

  override def readPartitionHourly(date: LocalDate,
                                   hours: Seq[Int],
                                   format: Option[String] = Some("tsv"))(implicit spark: SparkSession): Dataset[OnlineLogsRecord] = {
    val paths = (source match {
      case Some(DatasetSource.Logs) => hours.map(h => "%s/%s/%02d".format(basePath, date.format(logsDateFormatter), h))
      case _ => throw new NotImplementedError("Unsupported source for online logs")
    }).filter(FSUtils.directoryExists(_)(spark))

    format match {
      case Some("tsv") => spark
        .read
        .format("com.databricks.spark.csv")
        .option("sep", "\t")
        .option("header", "false")
        .load(paths.flatMap(x => FSUtils.listFiles(x, recursive = true)(spark).filter(x => x.contains(modelName.toLowerCase())).map(y => x + "/" + y)): _*)
        .toDF(typeOf[OnlineLogsRecord].members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => m.name.toString }: _*)
        .selectAs[OnlineLogsRecord]
      case _ => throw new NotImplementedError("Unsupported format for online logs")
    }
  }
}
