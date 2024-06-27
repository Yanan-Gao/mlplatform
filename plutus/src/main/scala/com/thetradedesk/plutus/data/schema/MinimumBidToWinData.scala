package com.thetradedesk.plutus.data.schema

import com.thetradedesk.plutus.data.loadCsvData
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

import java.time.LocalDateTime

final case class MinimumBidToWinData(BidRequestId: String,
                                     SupplyVendorLossReason: Int,
                                     LossReason: Int,
                                     WinCPM: Double,
                                     mbtw: Double)

case object MinimumBidToWinData {
  def loadRawMbtwData(dateTime: LocalDateTime): (Dataset[MinimumBidToWinData]) = {
    val rawMbtwData = loadCsvData[RawLostBidData](
      RawLostBidDataset.S3PATH,
      RawLostBidDataset.S3PATH_GEN,
      dateTime,
      RawLostBidDataset.SCHEMA
    )

    rawMbtwData.select(
      col("BidRequestId").cast(StringType),
      col("SupplyVendorLossReason").cast(IntegerType),
      col("LossReason").cast(IntegerType),
      col("WinCPM").cast(DoubleType),
      col("mbtw").cast(DoubleType)
    ).as[MinimumBidToWinData]
  }

}