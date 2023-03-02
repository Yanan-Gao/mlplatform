package com.thetradedesk.kongming.datasets

import com.thetradedesk._
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset

import java.time.LocalDate

object AdGroupPolicyDataset {
  val S3Path: String = "s3a://thetradedesk-useast-hadoop/cxw/foa/datasets/adgroupPolicies"

  def readHardCodedDataFrame(date: LocalDate): Dataset[AdGroupPolicySnapshotRecord] =  {
    spark.read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .csv(s"${S3Path}/date=${date.toString}")
      .selectAs[AdGroupPolicySnapshotRecord]
  }
}
