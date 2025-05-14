package com.thetradedesk.plutus.data.schema.shared
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import scala.util.hashing.MurmurHash3

object BackoffCommon {

  //Constants
  val bucketCount = 1000
  val platformWideBuffer = 0.50

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
}
