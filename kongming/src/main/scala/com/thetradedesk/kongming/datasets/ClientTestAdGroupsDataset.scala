package com.thetradedesk.kongming.datasets

import com.thetradedesk.geronimo.shared.shiftModUdf
import com.thetradedesk.spark.datasets.core.Csv
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.GenerateTrainSet.modelDimensions

final case class ClientTestAdGroupsIntIdRecord(
                                           Advertiser: String,
                                           BaseAdGroupId: String,
                                           BaseAdGroupIdInt: Int,
                                           TestAdGroupId: String,
                                           TestAdGroupIdInt: Int
                                                     )

case class ClientTestAdGroupsIntIdDataset() extends KongMingDataset[ClientTestAdGroupsIntIdRecord](
  s3DatasetPath = s"clienttestadgroups/v=1",
  fileFormat = Csv.WithHeader,
  experimentName = ""
){

  val S3Path: String = "s3a://thetradedesk-mlplatform-us-east-1/data/dev/kongming/testadgroups/"

  def readClientTestAdGroupsRecord(): Dataset[ClientTestAdGroupsIntIdRecord] = {
    val adgroupIdCardinality = modelDimensions(0).cardinality.getOrElse(0)

    val ClientTestAdGroupsIntId = spark.read.options(Map("header"->"true")).csv(s"${S3Path}")
      .withColumn("TestAdGroupIdInt", shiftModUdf(xxhash64(col("TestAdGroupId")), lit(adgroupIdCardinality)))
      .withColumn("BaseAdGroupIdInt", shiftModUdf(xxhash64(col("BaseAdGroupId")), lit(adgroupIdCardinality)))
      .selectAs[ClientTestAdGroupsIntIdRecord]

    ClientTestAdGroupsIntId

  }

}

