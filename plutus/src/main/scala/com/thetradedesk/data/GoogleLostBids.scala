package com.thetradedesk.data

import com.thetradedesk.data.schema.GoogleMinimumBidToWinDataset
import com.thetradedesk.data.schema.GoogleMinimumBidToWinData
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

object GoogleLostBids {

  /**
   * This will read in the minimum_bid_to_win data from google for the given date.
   * TODO: maybe have it spill over into the next day by 1 hour in case there are some trailing mb2w
   */
  def getLostBids(date: LocalDate): Dataset[GoogleMinimumBidToWinData] = {

    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(GoogleMinimumBidToWinDataset.SCHEMA)
      .load(getCleansedDataPath(GoogleMinimumBidToWinDataset.S3PATH, date))
      .filter(col("sv") === "google")
      // is only set for won bids or mode 79
      .filter((col("svLossReason") === "1") || (col("svLossReason") === "79"))
      .filter(col("winCPM") =!= 0.0 || col("mb2w") =!= 0.0)
      .select(
        col("BidRequestId").cast("String"),
        col("svLossReason").cast("Integer"),
        col("ttdLossReason").cast("Integer"),
        col("winCPM").cast("Double"),
        col("mb2w").cast("Double")
      ).as[GoogleMinimumBidToWinData]
  }
}
