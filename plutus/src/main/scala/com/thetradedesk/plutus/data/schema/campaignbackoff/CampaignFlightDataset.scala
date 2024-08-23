package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.{generateDataPathsDaily, getMaxDate, paddedDatePart}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.io.FSUtils.listDirectoryContents
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate

object CampaignFlightDataset {

  val S3PATH = f"${S3Roots.PROVISIONING_ROOT}/campaignflight/v=1/"

  def S3PATH_DATE_GEN = (date: LocalDate) => {
    f"date=${paddedDatePart(date)}"
  }


  def loadParquetCampaignFlightLatestPartitionUpTo(basePath: String, extGenerator: LocalDate => String, date: LocalDate, lookBack: Int = 0): Dataset[CampaignFlightRecord] = {
    import spark.implicits._

    val listEntries = listDirectoryContents(basePath)(spark)
    val listPaths = listEntries
      .filter(_.isDirectory)
      .map(_.getPath.toString)

    val maxDateOption = getMaxDate(listPaths, date)
    val maxDate = maxDateOption.getOrElse[LocalDate](date)
    println(s"CampaignFlightDataset ReadLatestPartitionUpTo MaxDate: $maxDate")

    // Finds and returns the latest date partition up to
    val paths = generateDataPathsDaily(basePath, extGenerator, maxDate, lookBack)

    // Ran into error due to flight end dates being outside Spark's timestamp range:
    // org.apache.spark.SparkUpgradeException: Error due to upgrading to Spark 3.0: Dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z from Parquet INT96 files may produce different results
    // To fix this: set a minimum end date for out of range dates and replace nulls with a future end date
    spark.read.parquet(paths: _*)
      .withColumn(
        "EndDateExclusiveUTC",
        when(col("EndDateExclusiveUTC") < "1900-01-01T00:00:00Z", "1901-01-01T00:00:00Z")
          .when(col("EndDateExclusiveUTC").isNull, "2099-12-22T23:59:59.000+00:00")
          .otherwise(col("EndDateExclusiveUTC"))
          .cast("timestamp")
      ).selectAs[CampaignFlightRecord]
  }

  // Handle flight end dates that are outside spark timestamp reading scope and nulls
  def loadParquetCampaignFlight(basePath: String, extGenerator: LocalDate => String, date: LocalDate, lookBack: Int = 0): Dataset[CampaignFlightRecord] = {
    val paths = generateDataPathsDaily(basePath, extGenerator, date, lookBack)
    import spark.implicits._
    spark.read.parquet(paths: _*)
      .withColumn(
        "EndDateExclusiveUTC",
        when(col("EndDateExclusiveUTC") < "1900-01-01T00:00:00Z", "1901-01-01T00:00:00Z")
          .when(col("EndDateExclusiveUTC").isNull, "2099-12-22T23:59:59.000+00:00")
          .otherwise(col("EndDateExclusiveUTC"))
          .cast("timestamp")
      ).selectAs[CampaignFlightRecord]
  }
}

case class CampaignFlightRecord(
                                 CampaignFlightId: Long,
                                 CampaignId: String,
                                 EndDateExclusiveUTC: Timestamp,
                                 BudgetInAdvertiserCurrency: BigDecimal,
                                 IsCurrent: Int
                               )



