package com.thetradedesk.kongming.datasets

import com.thetradedesk.kongming.{BaseFolderPath, MLPlatformS3Root}
import com.thetradedesk.spark.datasets.core._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.{ProdTesting, Testing}
import com.thetradedesk.spark.util.TTDConfig.environment

import java.time.LocalDate

final case class FeatureTableRecord(AdvertiserId: String,
                              CampaignId: String,
                              IndustryCategoryId: Option[Int],
                              AudienceId: Array[Int],
                             )

//todo this is an adhoc solution, will need to migrate to feature store pipeline
case class FeatureTableDataset(experimentOverride: Option[String] = None) extends DatePartitionedS3DataSet[FeatureTableRecord](
  GeneratedDataSet,
  "s3://thetradedesk-mlplatform-us-east-1/features/feature_store/",
  rootFolderPath = "profiles/source=provisioning/index=CampaignId/v=1/",
  partitionField = "date",
  fileFormat = Parquet,
  experimentOverride = experimentOverride,
  writeThroughHdfs = true)