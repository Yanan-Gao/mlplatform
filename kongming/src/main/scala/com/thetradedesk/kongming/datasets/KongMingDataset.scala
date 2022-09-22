package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{MLPlatformS3Root, getExperimentPath}
import com.thetradedesk.spark.datasets.core._

/**
 * This class serves as the basis for all date partitioned data sets in kongming for ease of reusability
 */
abstract class KongMingDataset[T <: Product : Manifest](dataSetType: DataSetType = GeneratedDataSet,
                                                        s3DatasetPath: String,
                                                        fileFormat: FileFormat = Parquet,
                                                        partitionField: String = "date",
                                                        experimentName: String = "")
  extends DatePartitionedS3DataSet[T](
    dataSetType = dataSetType,
    s3RootPath = MLPlatformS3Root,
    rootFolderPath = s"kongming/${getExperimentPath(experimentName)}${s3DatasetPath}",
    fileFormat = fileFormat,
    partitionField = partitionField,
    writeThroughHdfs = true
) {
}
