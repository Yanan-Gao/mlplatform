package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{MLPlatformS3Root, getExperimentPath}
import com.thetradedesk.spark.datasets.core.{Csv, DefaultTimeFormatStrings, GeneratedDataSet, PartitionColumnCalculation, PartitionedS3DataSet, PartitionedS3DataSet2, TFRecord}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

final case class BaseAssociateAdGroupMappingIntRecord(
                                                       AdGroupId: String,
                                                       AdGroupIdInt: Int,
                                                       BaseAdGroupId: String,
                                                       BaseAdGroupIdInt: Int
                                                     )
case class BaseAssociateAdGroupMappingIntDataset() extends KongMingDataset[BaseAssociateAdGroupMappingIntRecord](
  s3DatasetPath = s"baseAssociateAdgroup/v=1",
  fileFormat = Csv.WithHeader,
  experimentName = ""
)