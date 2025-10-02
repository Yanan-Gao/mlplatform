package com.thetradedesk

import com.thetradedesk.geronimo.shared._
import com.thetradedesk.spark.util.io.FSUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.LocalDate

package object frequency {

  object FrequencyDailyAggregationConstants {
    val Impression97 = 18
    val Impression98 = 25
    val Impression99 = 40
    val Impression999 = 100
    val Impression9999 = 250

    val ClickBotControlPoints: Seq[(Int, Double)] = Seq(
      Impression97 -> 0.90,
      Impression98 -> 0.75,
      Impression99 -> 0.60,
      Impression999 -> 0.50,
      Impression9999 -> 0.30
    )
  }

  def writeFrequencyData(df: DataFrame, outputPath: String, ttdEnv: String, outputPrefix: String, 
                        date: LocalDate, partitions: Int, outputFormat: String = "parquet", 
                        experimentName: String = null): Unit = {

    // note the date part is year=yyyy/month=m/day=d/
    val writer = df
      .repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      
    val writePath = if (experimentName == null) {
      s"$outputPath/$ttdEnv/$outputPrefix/${explicitDatePart(date)}"
    } else {
      s"$outputPath/$ttdEnv/experiment=$experimentName/$outputPrefix/${explicitDatePart(date)}"
    }

    outputFormat.toLowerCase match {
      case "parquet" => writer.parquet(writePath)
      case "csv" => writer.option("header", "true").csv(writePath)
      case _ => throw new IllegalArgumentException(s"Unsupported output format: $outputFormat. Supported: parquet, csv")
    }
  }

  def loadIsolatedAdvertiserFile(filePath: String)(implicit spark: SparkSession): DataFrame = {
    if (FSUtils.fileExists(filePath)) {
      spark.read.format("csv")
        .option("header", "false")
        .load(filePath)
        .withColumnRenamed("_c0", "AdvertiserId")
        .select("AdvertiserId")
        .distinct()
    } else {
      // Return empty dataframe if file doesn't exist
      spark.emptyDataFrame
    }
  }

  def getCountryFilter(countryFilePath: String)(implicit spark: SparkSession): DataFrame = {
    if (FSUtils.fileExists(countryFilePath)) {
      spark.read.format("csv")
        .option("header", "false") 
        .load(countryFilePath)
        .withColumnRenamed("_c0", "Country")
        .select("Country")
        .distinct()
    } else {
      throw new Exception(s"Country filter file does not exist at ${countryFilePath}")
    }
  }

  def debugInfo[T](varName: String, data: Dataset[T]): Unit = {
    println(s"$varName")
    println("---------------------------------------")
    data.printSchema()
    data.show(10, truncate = false)
    println("---------------------------------------")
  }
}