package com.thetradedesk.plutus.data.schema.shared
import com.thetradedesk.plutus.data.schema.campaignbackoff.CampaignThrottleMetricSchema
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, max, udf}

import java.sql.Timestamp
import scala.util.hashing.MurmurHash3

object BackoffCommon {

  //Constants
  val bucketCount = 1000
  val platformWideBuffer = 0.01
  val MinTestBucketIncluded = 0.0
  val MaxTestBucketExcluded = 0.9

  // Classes
  case class Campaign(CampaignId: String)
  case class CampaignFlightData(CampaignId: String, StartDateInclusiveUTC:Timestamp, EndDateExclusiveUTC:Timestamp)
  case class PacingData(CampaignId: String, IsValuePacing: Boolean)

  // Methods
  def computeBudgetBucketHash(entityId: String, bucketCount: Int): Short = {
    // Budget Bucket Hash Function
    // Tested here: https://dbc-7ae91121-86fd.cloud.databricks.com/editor/notebooks/1420435698064795?o=2560362456103657#command/1420435698066605
    Math.abs(MurmurHash3.stringHash(entityId) % bucketCount).toShort
  }
  val getTestBucketUDF = udf(computeBudgetBucketHash(_: String, _: Int))

  def handleDuplicateCampaignFlights(campaignThrottleData: Dataset[CampaignThrottleMetricSchema]): Dataset[CampaignThrottleMetricSchema]  = {
    // Get the most recent campaign flight when duplicate campaigns
    val mostRecentCampaignFlights = campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        max("CampaignFlightId").as("CampaignFlightId")
      ).select("CampaignFlightId")

    campaignThrottleData
      .join(mostRecentCampaignFlights, Seq("CampaignFlightId"), "inner")
      .as[CampaignThrottleMetricSchema]
  }
}
