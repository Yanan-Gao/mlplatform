package com.thetradedesk.frequency.schema

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet

case class AdGroupRecord(AdGroupId: String, AudienceId: String, IndustryCategoryId: String, ROIGoalTypeId: String, CampaignId: String)

case class AdGroupDataSet() extends ProvisioningS3DataSet[AdGroupRecord]("adgroup/v=1", true) {}

case class AdGroupDisabledRecentDataSet() extends ProvisioningS3DataSet[AdGroupRecord]("adgroupdisabledrecent/v=1") {}

case class UnifiedAdGroupDataSet() {
  private def filterLatestUpdate(adGroupDS: Dataset[AdGroupRecord]): Dataset[AdGroupRecord] = {
    val agWindow = Window.partitionBy($"AdGroupId").orderBy($"LastUpdatedAt".desc, $"date".desc)
    adGroupDS
      .withColumn("RowNumber", row_number().over(agWindow))
      .where($"RowNumber" === lit(1))
      .drop("RowNumber")
      .as[AdGroupRecord]
  }

  def readLatestPartitionUpTo(maxInclusivePartition: java.time.LocalDate,
                              isInclusive: Boolean = false,
                              verbose: Boolean = false): Dataset[AdGroupRecord] = {
    filterLatestUpdate(
      AdGroupDataSet().readLatestPartitionUpTo(maxInclusivePartition, isInclusive, verbose)
        .union(AdGroupDisabledRecentDataSet().readLatestPartitionUpTo(maxInclusivePartition, isInclusive, verbose)))
  }

  def readLatestPartition(verbose: Boolean = true): Dataset[AdGroupRecord] = {
    filterLatestUpdate(
      AdGroupDataSet().readLatestPartition(verbose)
        .union(AdGroupDisabledRecentDataSet().readLatestPartition(verbose))
    )
  }
}


