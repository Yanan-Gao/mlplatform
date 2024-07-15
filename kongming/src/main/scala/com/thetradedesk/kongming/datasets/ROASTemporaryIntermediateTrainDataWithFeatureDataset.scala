package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root, writeThroughHdfs}
import com.thetradedesk.spark.datasets.core._

import java.time.LocalDate

case class ROASTemporaryIntermediateTrainDataWithFeatureDataset(split:String, experimentOverride: Option[String]=None) extends PartitionedS3DataSet2[DataForModelTrainingRecord, LocalDate, String, LocalDate, String](
  GeneratedDataSet,
  MLPlatformS3Root,
  rootFolderPath =  s"${BaseFolderPath}/intermediatetraindatawithfeaturedataset/v=1/split=${split}",
  "date" -> ColumnExistsInDataSet,
  "biddate" -> ColumnExistsInDataSet,
  fileFormat = Parquet,
  experimentOverride = experimentOverride,
  writeThroughHdfs = writeThroughHdfs,
  throwIfSourceEmpty = split != "untracked"
){
  override def toStoragePartition1(value1: LocalDate): String = value1.format(DefaultTimeFormatStrings.dateTimeFormatter)
  override def toStoragePartition2(value2: LocalDate): String = value2.format(DefaultTimeFormatStrings.dateTimeFormatter)
}
