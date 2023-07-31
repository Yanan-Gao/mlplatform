package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{MLPlatformS3Root, writeThroughHdfs}
import com.thetradedesk.spark.datasets.core.PartitionedS3DataSet.buildPath
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.util.{ProdTesting, Testing}
import com.thetradedesk.spark.util.TTDConfig.{config, environment}
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql.Dataset

import java.time.LocalDate

/**
 * This class serves as the basis for all date partitioned data sets in kongming for ease of reusability
 */
abstract class KongMingDataset[T <: Product : Manifest](dataSetType: DataSetType = GeneratedDataSet,
                                                        s3DatasetPath: String,
                                                        fileFormat: FileFormat = Parquet,
                                                        partitionField: String = "date",
                                                        experimentOverride: Option[String] = None)
  extends DatePartitionedS3DataSet[T](
    dataSetType = dataSetType,
    s3RootPath = MLPlatformS3Root,
    rootFolderPath = s"kongming/${s3DatasetPath}",
    fileFormat = fileFormat,
    partitionField = partitionField,
    writeThroughHdfs = writeThroughHdfs,
    experimentOverride = experimentOverride
  ) {
  def writePartition(dataSet: Dataset[T], partition: LocalDate, coalesceToNumFiles: Option[Int]): Long = {
    // write to and read from test folders for ProdTesting environment to get the correct row count
    val isProdTesting = environment == ProdTesting
    if (isProdTesting) {
      environment = Testing
    }
    // write partition to S3 folders
    var count = super.writePartition(dataSet, partition, coalesceToNumFiles)
    // recount the rows in partition if writePartition returns 0 and the partition actually exists
    if(count == 0 && partitionExists(partition)) {
      count = readPartition(partition).count()
    }
    // set environment back to ProdTesting after getting the dataset count if the environment is actually ProdTesting
    if (isProdTesting) {
      environment = ProdTesting
    }
    count
  }

  override def partitionExists(partition: LocalDate): Boolean = {
    val path = buildPath(
      rootFolderPath,
      partitionField -> partition.format(dateTimeFormat)
    )
    FSUtils.directoryExists(s"$readRoot$path")
  }

}
