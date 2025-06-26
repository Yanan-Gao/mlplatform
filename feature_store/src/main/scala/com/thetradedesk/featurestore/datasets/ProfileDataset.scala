package com.thetradedesk.featurestore.datasets

import com.thetradedesk.featurestore._
import com.thetradedesk.featurestore.constants.FeatureConstants.{GrainDay, GrainHour}
import com.thetradedesk.featurestore.utils.{PathUtils, StringUtils}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.{Dataset, SaveMode}


case class ProfileDataset(rootPath: String = MLPlatformS3Root,
                          prefix: String,
                          grain: Option[String] = None,
                          overrides: Map[String, String]) extends ProfileBaseDataset(
  rootPath,
  prefix,
  grain,
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
                                  grain: Option[String] = None,
                                  parameters: Map[String, String]) {

  lazy val datasetPath: String = StringUtils.applyNamedFormat(getPrefixFormat, parameters)

  def getPrefixFormat: String = {
    var prefixFormat = PathUtils.concatPath(rootPath, prefix)
    if (grain.isDefined) {
      val datePartition = grain.get match {
        case GrainDay => "/date={dateStr}"
        case GrainHour => "/date={dateStr}/hour={hourInt}"
        case _ => throw new RuntimeException(s"Unsupported grain type ${grain}")
      }
      prefixFormat = PathUtils.concatPath(prefixFormat, datePartition)
    }
    prefixFormat
  }

  def writePartition(dataset: Dataset[_],
                     numPartitions: Option[Int] = None,
                     format: Option[String] = Some("parquet"),
                     saveMode: SaveMode = SaveMode.Overwrite
                    ): Long = {
    val writeThroughHdfs = ttdEnv != "local"

    DatasetWriter.writeDataSet(dataset,
      datasetPath,
      None, // let spark decide
      format = format,
      saveMode = saveMode,
      writeThroughHdfs = writeThroughHdfs
    )

    dataset.count()
  }
}