package com.thetradedesk.plutus.data.transform.dashboard

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.{envForRead, envForWrite, loadParquetDataDailyV2}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object CampaignDAPlutusDashboardDataTransform extends Logger {

  val spark = TTDSparkContext.spark

  import spark.implicits._

  val numPartitions = 200

  def getCampaignDAPlutusMetrics(pcResultsMerged: Dataset[PcResultsMergedSchema],
                                 adGroupData: Dataset[AdGroupRecord]
                                ): Dataset[CampaignDAPlutusDashboardSchema] = {

    val pcResultsMergedDataset = pcResultsMerged.withColumnRenamed("PredictiveClearingEnabled", "PCResults_PredictiveClearingEnabled")
    val ag = adGroupData.select(col("AdGroupId"), col("PredictiveClearingEnabled"))

    val df = pcResultsMergedDataset.join(broadcast(ag), Seq("AdGroupId"), "left")
      .withColumn("Date", to_date(col("LogEntryTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").cast(DateType))
      .withColumn(
        "FinalBidPrice",
        when(col("FinalBidPrice").isNull, col("AdjustedBidCPMInUSD") * coalesce(col("BidsFirstPriceAdjustment"), lit(1)))
          .otherwise(col("FinalBidPrice"))
      ).withColumn(
        "S3prov_PredictiveClearingEnabled",
        col("PredictiveClearingEnabled")
      ).withColumn(
        "PredictiveClearingEnabled", // heuristic to account for nulls when joining to s3 prov ag table
        when(col("PredictiveClearingMode") === 0 && col("BidsFirstPriceAdjustment").isNull && (col("Model") === "noPcApplied" || col("Model").isNull), false)
          .when(col("PredictiveClearingMode") === 1 && col("BidsFirstPriceAdjustment").isNull, true)
          .when(col("PredictiveClearingMode") === 3 && !col("BidsFirstPriceAdjustment").isNull && (col("Model") =!= "noPcApplied" || col("Model").isNull), true)
          .otherwise(col("S3prov_PredictiveClearingEnabled"))
      ).drop(
        col("Channel")
      )

    // Aggregate metrics
    val generalMetrics_df = generalMetricsDA(df)

    generalMetrics_df.selectAs[CampaignDAPlutusDashboardSchema]
  }

  // Aggregate general spend/bid metrics
  def generalMetricsDA(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("Date", "CampaignId", "AdGroupId")

    df
      .withColumnRenamed("ChannelSimple", "Channel")
      .withColumnRenamed("Strategy", "PushdownDial")
      .groupBy(
        "Date", "CampaignId", "AdGroupId", "Channel", "DetailedMarketType", "PredictiveClearingMode", "PredictiveClearingEnabled", "IsValuePacing"
      ).agg(
        count(when(col("IsImp"), "*")).alias("ImpressionCount"),
        count("*").alias("BidCount"),
        sum(col("MediaCostCPMInUSD")).alias("MediaCostCPMInUSD"),
        sum(col("PartnerCostInUSD")).alias("PartnerCostInUSD"),
        sum(col("AdvertiserCostInUSD")).alias("AdvertiserCostInUSD"),
        sum(col("FeeAmount")).alias("FeeAmount"),
        sum(col("AdjustedBidCPMInUSD")).alias("InitialBid"),
        sum(col("BidsFirstPriceAdjustment")).alias("FirstPriceAdjustment"),
        sum(col("FinalBidPrice")).alias("FinalBid"),
        sum(col("PushdownDial")).alias("Avg_PushdownDial") // to account for different strategies across same adgroup
      ).withColumn(
        "AggPredictiveClearingEnabled",
        max(col("PredictiveClearingEnabled")).over(windowSpec)
      )
  }

  def get_prov_joineddays[T <: Product : Manifest](dataset: ProvisioningS3DataSet[T], date: LocalDate, id: String): Dataset[T] = {
    // S3 provisioning takes a snapshot in the beginning of the day (1/2 UTC) so changes made to prov settings later in the day get excluded in join
    // Workaround: joining two days of S3 provisioning data to account for what's missed after the snapshot period
    val sameday = dataset.readDate(date)
    val dayafter = dataset.readDate(date.plusDays(1))
    val joined_df = sameday.alias("same")
      .join(dayafter.alias("after"), Seq(id), "outer")

    val columns = sameday.columns.map(colName =>
      coalesce(col(s"after.$colName"), col(s"same.$colName")).alias(colName)
    )

    val final_df = joined_df.select(columns: _*)

    final_df.selectAs[T]
  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    val pcResultsMergedData = loadParquetDataDailyV2[PcResultsMergedSchema](
      PcResultsMergedDataset.S3_PATH(Some(envForRead)),
      PcResultsMergedDataset.S3_PATH_DATE_GEN,
      date,
      nullIfColAbsent = true
    )

    val adGroupData = get_prov_joineddays(AdGroupDataSet(), date, "AdGroupId")

    val results_daplutusmetrics = getCampaignDAPlutusMetrics(pcResultsMergedData, adGroupData)

    val outputPath = CampaignDAPlutusDashboardDataset.S3_PATH_DATE(date, envForWrite)
    results_daplutusmetrics.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
