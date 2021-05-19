package com.thetradedesk.data

import java.time.LocalDate

import com.thetradedesk.data.schema.DiscrepancyDataset
import com.thetradedesk.data.schema.{Deals, Pda, Svb}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col

class Discrepancy {

  def getDiscrepancyData(date: LocalDate, lookBack: Int)(implicit spark: SparkSession) = {
    import spark.implicits._
    val svbDf = getParquetData[Svb](date, lookBack, DiscrepancyDataset.SBVS3)
      .withColumn("SupplyVendor" , col("RequestName"))
      .withColumn("svbDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("RequestName")


    val pdaDf = getParquetData[Pda](date, lookBack, DiscrepancyDataset.PDAS3)
      .withColumn("SupplyVendor" , col("SupplyVendorName"))
      .withColumn("pdaDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("SupplyVendorName")

    val dealDf = getParquetData[Deals](date, lookBack, DiscrepancyDataset.DEALSS3)

    (svbDf, pdaDf, dealDf)
  }

}
