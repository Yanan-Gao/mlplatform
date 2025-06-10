package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.datasets.metadata.{AdvertiserExcludeRecord, ClickPartnerExcludeList, DaRestrictedAdvertiserDataset}
import com.thetradedesk.featurestore.{aggLevel, shouldTrackTDID}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate

// this job has skew data issue, need to fix
// https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusterDetails/j-34WSEG9VA2R3Z
object AggClickBidFeedback extends FeatureStoreAggJob {

  override val sourcePartition: String = "clickbidfeedback"

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    val clickBidFeedbackDataset = DailyClickBidFeedbackDataset()
    if(!clickBidFeedbackDataset.isProcessed(date)) {
      throw new RuntimeException(s"Success file not found for DailyClickBidFeedbackDataset on date: ${date}, at location: ${clickBidFeedbackDataset.getSuccessFilePath(date)}")
    }

    // exclude partners that don't share click data, and MedHealth data
    val advertiserToExclude = ClickPartnerExcludeList.readAdvertiserList()
      .union(
        DaRestrictedAdvertiserDataset().readLatestPartitionUpTo(date, true)
          .filter(($"CategoryPolicy" === lit("Health")) && ($"IsRestricted" === lit(1)))
          .selectAs[AdvertiserExcludeRecord]
      )

    clickBidFeedbackDataset.readPartition(date = date, lookBack = Some(lookBack))
      .filter(shouldTrackTDID(col(aggLevel)))
      .join(advertiserToExclude, Seq("AdvertiserId"), "left_anti")
      .withColumn("Click", when(col("ClickRedirectId").isNotNull, lit(1)).otherwise(0))
      .withColumn("Impression", when(col("BidFeedbackId").isNotNull, lit(1)).otherwise(0))
  }
}