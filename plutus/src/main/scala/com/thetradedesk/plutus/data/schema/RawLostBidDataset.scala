package com.thetradedesk.plutus.data.schema

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

final case class MinimumBidToWinData(BidRequestId: String,
                                     SupplyVendorLossReason: Int,
                                     LossReason: Int,
                                     WinCPM: Double,
                                     mbtw: Double)

final case class RawLostBidData(
                                 LogEntryTime: String,
                                 BidRequestId: String,
                                 CreativeId: String,
                                 AdGroupId: String,
                                 CampaignId: String,
                                 PrivateContractId: String,
                                 PartnerId: String,
                                 AdvertiserId: String,
                                 CampaignFlightId: String,
                                 SupplyVendorLossReason: Integer,
                                 LossReason: Integer,
                                 WinCPM: Double,
                                 SupplyVendor: String,
                                 BidRequestTime: String,
                                 mbtw: Double
                               )

trait RawLostBidSchema {
  def SCHEMA: StructType = new StructType()
    .add("LogEntryTime", StringType, true) //0
    .add("BidRequestId", StringType, true) // 1
    .add("CreativeId", StringType, true) // 2
    .add("AdGroupId", StringType, true) // 3
    .add("CampaignId", StringType, true) // 4
    .add("PrivateContractId", StringType, true) // 5
    .add("PartnerId", StringType, true) // 6
    .add("AdvertiserId", StringType, true) // 7
    .add("CampaignFlightId", StringType, true) // 8
    .add("SupplyVendorLossReason", IntegerType, true) //9
    .add("LossReason", IntegerType, true) // 10
    .add("WinCPM", DoubleType, true) // 11
    .add("SupplyVendor", StringType, true) // 12
    .add("BidRequestTime", StringType, true) // 13
    .add("mbtw", DoubleType, true) // 14
}

object RawLostBidDataset extends RawLostBidSchema {
  def S3PATH = "s3://thetradedesk-useast-logs-2/lostbidrequest/cleansed/"
}

