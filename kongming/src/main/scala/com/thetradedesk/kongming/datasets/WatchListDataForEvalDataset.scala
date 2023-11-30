package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core.{Csv, GeneratedDataSet}


case class WatchListDataForEvalDataset(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[OutOfSampleAttributionRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/watchlistdata/trainset/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}

case class WatchListOosDataForEvalDataset(experimentOverride: Option[String] = None)
  extends DateSplitPartitionedS3Dataset[OutOfSampleAttributionRecord](
    GeneratedDataSet, MLPlatformS3Root, s"${BaseFolderPath}/watchlistdata/oos/v=1",
    fileFormat = Csv.WithHeader,
    experimentOverride = experimentOverride
  ) {
}