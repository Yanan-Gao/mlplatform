package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.utils.{PathUtils, StringUtils}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode}


case class ProfileDataset(rootPath: String = MLPlatformS3Root,
                          prefix: String,
                          overrides: Map[String, String]) extends ProfileBaseDataset(
  rootPath,
  prefix,
  overrides
) {

  def writeWithRowCountLog(dataset: Dataset[_], numPartitions: Option[Int] = None): (String, Long) = {
    val rows = super.writePartition(dataset, numPartitions)
    val logName = StringUtils.applyNamedFormat("source={sourcePartition}/index={indexPartition}/job={jobName}", overrides)
    (logName, rows)
  }

  def isProcessed: Boolean = {
    val successFile = s"${datasetPath}/_SUCCESS"
    FSUtils.fileExists(successFile)
  }
}

abstract class ProfileBaseDataset(rootPath: String,
                                  prefix: String,
                                  parameters: Map[String, String]) {
  lazy val datasetPath: String = StringUtils.applyNamedFormat(PathUtils.concatPath(rootPath, prefix), parameters)

  def writePartition(dataset: Dataset[_],
                     numPartitions: Option[Int] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.Overwrite
                    ): Long = {

    DatasetWriter.writeDataSet(dataset,
      datasetPath,
      None, // let spark decide
      format = format,
      saveMode = saveMode,
      writeThroughHdfs = true
    )

    dataset.count()
  }
}
