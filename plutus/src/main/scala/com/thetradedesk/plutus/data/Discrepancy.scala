package com.thetradedesk.plutus.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import java.time.LocalDate

import com.thetradedesk.plutus.data.schema.{Deals, DiscrepancyDataset, Pda, Svb}

object Discrepancy {

  def getDiscrepancyData(date: LocalDate)(implicit spark: SparkSession) = {
    import spark.implicits._
    val svbDf = getParquetData[Svb](DiscrepancyDataset.SBVS3, date)
      .withColumn("SupplyVendor" , col("RequestName"))
      .withColumn("svbDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("RequestName")


    val pdaDf = getParquetData[Pda](DiscrepancyDataset.PDAS3, date)
      .withColumn("SupplyVendor" , col("SupplyVendorName"))
      .withColumn("pdaDiscrpancyAdjustment" , col("DiscrepancyAdjustment"))
      .drop("DiscrepancyAdjustment")
      .drop("SupplyVendorName")

    val dealDf = getParquetData[Deals](DiscrepancyDataset.DEALSS3, date)
      .join(svbDf, "SupplyVendorId")
      .select(col("SupplyVendor"), col("SupplyVendorDealCode").alias("DealId"), col("IsVariablePrice"))

    (svbDf, pdaDf, dealDf)
  }
}
