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
      .withColumn("svbDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")


    val pdaDf = getParquetData[Pda](date, lookBack, DiscrepancyDataset.PDAS3)
      .withColumn("pdaDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")

    val dealDf = getParquetData[Deals](date, lookBack, DiscrepancyDataset.DEALSS3)
      .join(svbDf, "SupplyVendorId")
      .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))

    (svbDf, pdaDf, dealDf)
  }

}
