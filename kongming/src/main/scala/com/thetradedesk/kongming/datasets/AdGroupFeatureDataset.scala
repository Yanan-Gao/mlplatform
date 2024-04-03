package com.thetradedesk.kongming.datasets

import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}

final case class AudienceFeatureRecord(
                                       AdvertiserId: String,
                                       CampaignId: String,
                                       AudienceId: String
                                     )

/** Only includes Enabled AdGroups */
case class AdGroupFeatureDataSet() extends ProvisioningS3DataSet[AudienceFeatureRecord]("adgroup/v=1", true) {}



case class UnifiedAdGroupFeatureDataSet() {
  /**
   * Since the export for each of these data sets can occur at different times, there may be duplicate rows
   * because AdGroups may turn on or off. Account for that by using the latest LastUpdatedAt.
   * Note that Enabling/Disabling Ad Groups does not trigger LastUpdatedAt, so this would just pick a random row.
   *
   * @param adGroupDS Data set of Ad Groups with potentially duplicate rows
   * @return Deduplicated data set of Ad Groups using the latest updated date
   */
  private def filterLatestUpdate(adGroupDS: Dataset[AudienceFeatureRecord]): Dataset[AudienceFeatureRecord] = {
    val agWindow = Window.partitionBy($"AdGroupId").orderBy($"LastUpdatedAt".desc, $"date".desc)
    adGroupDS
      .withColumn("RowNumber", row_number().over(agWindow))
      .where($"RowNumber" === lit(1))
      .drop("RowNumber")
      .as[AudienceFeatureRecord]
  }

  def readLatestPartitionUpTo(maxInclusivePartition: java.time.LocalDate,
                              isInclusive: Boolean = false,
                              verbose: Boolean = false): Dataset[AudienceFeatureRecord] = {
    filterLatestUpdate(
      AdGroupFeatureDataSet().readLatestPartitionUpTo(maxInclusivePartition, isInclusive, verbose)
        .union(AdGroupDisabledRecentDataSet().readLatestPartitionUpTo(maxInclusivePartition, isInclusive, verbose).as[AudienceFeatureRecord]
        ))
  }
}