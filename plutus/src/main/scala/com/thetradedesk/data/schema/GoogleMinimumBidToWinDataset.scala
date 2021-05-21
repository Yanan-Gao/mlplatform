package com.thetradedesk.data.schema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

final case class GoogleMinimumBidToWinData(bidRequestId: String,
                                              svLossReason: Int,
                                              ttdLossReason: Int,
                                              winCPM: Double,
                                              mb2w: Double)

object GoogleMinimumBidToWinDataset {

  def S3PATH = "s3://thetradedesk-useast-logs-2/lostbidrequest/cleansed/"

  def SCHEMA: StructType = new StructType()
    .add("LogEntryTime", StringType, true) //0
    .add("BidRequestId", StringType, true) // 1
    .add("creativeId", StringType, true) // 2
    .add("adgroupId", StringType, true) // 3
    .add("throwAway_4", StringType, true) // 4
    .add("throwAway_5", StringType, true) // 5
    .add("TtdPartnerId", StringType, true) // 6
    .add("throwAway_7", StringType, true) // 7
    .add("throwAway_8", StringType, true) // 8
    .add("svLossReason", IntegerType, true) //9
    .add("ttdLossReason", IntegerType, true) // 10
    .add("winCPM", DoubleType, true) // 11
    .add("sv", StringType, true) // 12
    .add("bidRequestTime", StringType, true) // 13
    .add("mb2w", DoubleType, true) // 14

}

