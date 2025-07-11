package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}

final case class AdGroupRecord(AdGroupId: String,
                               AdGroupIdInteger: Int,
                               CampaignId: String,
                               ROIGoalTypeId: Int
                              )

// probably won't need this for bid request filter, layer it in during training data generation
/** Only includes Enabled AdGroups */
case class AdGroupDataSet() extends ProvisioningS3DataSet[AdGroupRecord]("adgroup/v=1", true) {}

/** Disabled and Recent (within 45 days noninclusive) AdGroups */
case class AdGroupDisabledRecentDataSet() extends ProvisioningS3DataSet[AdGroupRecord]("adgroupdisabledrecent/v=1") {}

/** Includes disabled and recent as well as enabled AdGroups */
case class UnifiedAdGroupDataSet() {
  /**
   * Since the export for each of these data sets can occur at different times, there may be duplicate rows
   * because AdGroups may turn on or off. Account for that by using the latest LastUpdatedAt.
   * Note that Enabling/Disabling Ad Groups does not trigger LastUpdatedAt, so this would just pick a random row.
   * @param adGroupDS Data set of Ad Groups with potentially duplicate rows
   * @return Deduplicated data set of Ad Groups using the latest updated date
   */
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
