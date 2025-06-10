package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.utils.PathUtils
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}


case class ProfileDataset(dataSetPath: String = ProfileDataBasePath,
                          rootPath: String = MLPlatformS3Root,
                          sourcePartition: String,
                          indexPartition: String,
                          jobName: String,
                          environment: String = ttdEnv) extends ProfileBaseDataset(
  dataSetPath,
  rootPath,
  environment
) {
  override val subPartitions: String =
    s"source=${sourcePartition}/index=${indexPartition}/job=${jobName}/v=1"

  def writeWithRowCountLog(dataset: Dataset[_], datePartition: Any = null, numPartitions: Option[Int] = None): (String, Long) = {
    val rows = super.writePartition(dataset, datePartition, numPartitions)
    val logName = s"source=${sourcePartition}/index=${indexPartition}/job=${jobName}"
    (logName, rows)
  }

  def isProcessed(date: LocalDate): Boolean = {
    val successFile = s"${getWritePath(date)}/_SUCCESS"
    FSUtils.fileExists(successFile)
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

  def getWritePath(datePartition: Any = null): String = {
    val datePath: String = datePartitionedPath(Option.apply(datePartition))
    val writeEnv = if (environment == "prodTest") "test" else environment
    Array(rootPath, writeEnv, dataSetPath, subPartitions, datePath).reduce(PathUtils
      .concatPath)
  }

  def writePartition(dataset: Dataset[_],
                     datePartition: Any = null,
                     numPartitions: Option[Int] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.Overwrite
                    ): Long = {

    val writePath = getWritePath(datePartition)

    DatasetWriter.writeDataSet(dataset,
      writePath,
      None, // let spark decide
      format = format,
      saveMode = saveMode,
      writeThroughHdfs = true
    )

    dataset.count()
  }
}
