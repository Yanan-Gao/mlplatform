package com.thetradedesk.data

import com.thetradedesk.data.schema.MinimumBidToWinDataset
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

import java.time.LocalDate

class GoogleLostBids {

  def getLostBids(date: LocalDate, lookBack: Int)(implicit spark: SparkSession): Dataset[MinimumBidToWinDataset] = {

    import spark.implicits._

    val lostBidPaths = (1 to lookBack).map { i =>
      f"${MinimumBidToWinDataset.S3PATH}/${datePaddedPart(date.minusDays(i))}/*/*/*.gz"
    }

    spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("mode", "DROPMALFORMED")
      .schema(MinimumBidToWinDataset.SCHEMA)
      .load(lostBidPaths: _*)
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
      ).as[MinimumBidToWinDataset]
  }
}