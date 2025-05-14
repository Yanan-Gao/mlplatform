package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset
import com.thetradedesk.plutus.data.{generateDataPathsDaily, getMaxDate, paddedDatePart}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.datasets.core.S3Roots
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.io.FSUtils.listDirectoryContents
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate

object CampaignFlightDataset extends S3DailyParquetDataset[CampaignFlightRecord] {
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String =
    f"${S3Roots.PROVISIONING_ROOT}/campaignflight/v=1"

  // Handle flight end dates that are outside spark timestamp reading scope and nulls
  override def readDate(date: LocalDate, env: String, lookBack: Int, nullIfColAbsent: Boolean)
                       (implicit encoder: Encoder[CampaignFlightRecord], spark: SparkSession): Dataset[CampaignFlightRecord] = {
    super.readDate(date, env, lookBack, nullIfColAbsent)
      .withColumn(
        "EndDateExclusiveUTC",
        when(col("EndDateExclusiveUTC") < "1900-01-01T00:00:00Z", "1901-01-01T00:00:00Z")
          .when(col("EndDateExclusiveUTC").isNull, "2099-12-22T23:59:59.000+00:00")
          .otherwise(col("EndDateExclusiveUTC"))
          .cast("timestamp")
      ).selectAs[CampaignFlightRecord]
  }

  // env defaults to "" because its not used for this dataset
  override def readLatestDataUpToIncluding(maxDate: LocalDate, env: String = "", lookBack: Int = 0, nullIfColAbsent: Boolean = false)
                                          (implicit encoder: Encoder[CampaignFlightRecord], spark: SparkSession): Dataset[CampaignFlightRecord] = {
    super.readLatestDataUpToIncluding(maxDate, env, lookBack, nullIfColAbsent)
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
                                 StartDateInclusiveUTC: Timestamp,
                                 EndDateExclusiveUTC: Timestamp,
                                 BudgetInAdvertiserCurrency: BigDecimal,
                                 IsCurrent: Int
                               )



