package com.thetradedesk.data

import java.time.LocalDate

import com.thetradedesk.spark.sql.SQLFunctions.ColumnExtensions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, round, when}

class CleanedData {

  def createCleanDataset(date: LocalDate, mbwRatio: Double, dataRoot: String, folderName: String, svName: String)(implicit spark: SparkSession) = {

    val d = datePaddedPart(date)
    val df = spark.read.parquet(s"$dataRoot/$svName/$d")
      .withColumn("is_imp", when(col("RealMediaCost").isNotNull, 1.0).otherwise(0.0))
    // need this as was int in previous query - making it a float will make things easier later on

    val df_clean = df
      .filter((col("mb2w").isNotNull))
      .withColumn("valid",
        // there are cases that dont make sense - we choose to remove these for simplicity while we investigate further.

        // impressions where MB2W < Media Cost and MB2W is above a floor (if present)
        when((
          (col("is_imp") === 1.0) &&
            (col("mb2w") <= col("RealMediaCost")) &&
            ((col("mb2w") >= col("FloorPriceInUSD")) || col("FloorPriceInUSD").isNullOrEmpty )
          ), true)
          // Bids where MB2W is > bid price AND not an extreme value AND is above floor (if present)
          .when((
            (col("is_imp") === 0.0) &&
              (col("mb2w") > col("b_RealBidPrice")) &&
              ((col("FloorPriceInUSD").isNullOrEmpty) || (col("mb2w") >= col("FloorPriceInUSD"))) &&
              (round(col("b_RealBidPrice") / col("mb2w"), 1) > mbwRatio) &&
              ((col("mb2w") > col("FloorPriceInUSD")) || (col("FloorPriceInUSD").isNullOrEmpty))
            ), true)
          .otherwise(false)
      )
      .filter(col("valid") === true)
      .drop("valid")

    /*println(f"Total Data: ${df_clean.count}")
    println(f"Total Data with MB2W: ${df_clean.filter(col("mb2w").isNotNull).count}")
    println(f"Total Bids with MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).count}")
    println(f"Total Imps with MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).count}")
    println(f"Total Bids with Valid MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNull)).filter(col("mb2w") >= col("b_RealBidPrice")).count}")
    println(f"Total Imps with Valid MB2W: ${df_clean.filter((col("mb2w").isNotNull)&&(col("RealMediaCost").isNotNull)).filter(col("mb2w") <= col("RealMediaCost")).count}")
*/
    println("ouputting clean data to:\n" + s"$dataRoot/$folderName/$d")

    df_clean.repartition(200).write.mode(SaveMode.Overwrite).parquet(s"$dataRoot/$folderName/$d")

    df
  }


}
