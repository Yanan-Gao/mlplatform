package com.thetradedesk.plutus.data.transform.dashboard

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.{envForRead, envForWrite, loadParquetDataDailyV2}
import com.thetradedesk.spark.TTDSparkContext
import com.thetradedesk.spark.datasets.core.ProvisioningS3DataSet
import com.thetradedesk.spark.datasets.sources.{AdGroupRecord, _}
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.time.LocalDate

object DAPlutusDashboardDataTransform extends Logger {

  val spark: SparkSession = TTDSparkContext.spark

  import spark.implicits._

  def getDAPlutusMetrics(pcResultsMerged: Dataset[PcResultsMergedDataset],
                         roiGoalTypeData: Dataset[ROIGoalTypeRecord],
                         adGroupData: Dataset[AdGroupRecord],
                         campaignData: Dataset[CampaignRecord],
                        ): Dataset[DAPlutusDashboardSchema] = {

    val dataset_roi_ag = broadcast(adGroupData.join(broadcast(roiGoalTypeData), Seq("ROIGoalTypeId"), "left")
      .select("AdGroupId", "PredictiveClearingEnabled", "ROIGoalTypeName"))

    val dataset_campaigns = broadcast(campaignData.select("CampaignId", "IsManagedByTTD"))
    var df = pcResultsMerged
      .join(dataset_roi_ag, Seq("AdGroupId"), "left")
      .join(dataset_campaigns, Seq("CampaignId"), "left")

    df = df
      .withColumn("Date", to_date(col("LogEntryTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").cast(DateType))
      .withColumn("MarketType", when(col("DetailedMarketType") === "Open Market", lit("OM")).otherwise(lit("PMP")))
      .withColumn(
        "FinalBidPrice",
        when(col("FinalBidPrice").isNull, col("AdjustedBidCPMInUSD") * coalesce(col("BidsFirstPriceAdjustment"), lit(1)))
          .otherwise(col("FinalBidPrice"))
      ).withColumn(
        "S3prov_PredictiveClearingEnabled",
        col("PredictiveClearingEnabled")
      ).withColumn(
        "PredictiveClearingEnabled", // heuristic to account for nulls and midday setting changes when joining to s3 prov ag table
        when(col("PredictiveClearingMode") === 0 && col("BidsFirstPriceAdjustment").isNull && (col("Model") === "noPcApplied" || col("Model").isNull), false)
          .when(col("PredictiveClearingMode") === 1 && col("BidsFirstPriceAdjustment").isNull, true)
          .when(col("PredictiveClearingMode") === 3 && !col("BidsFirstPriceAdjustment").isNull && (col("Model") =!= "noPcApplied" || col("Model").isNull), true)
          .otherwise(col("S3prov_PredictiveClearingEnabled"))
      ).withColumn(
        "Bin_InitialBid", binPriceColumn("AdjustedBidCPMInUSD")
      ).withColumn(
        "Bin_FirstPriceAdjustment", binPCColumn("BidsFirstPriceAdjustment")
      ).drop(
        col("Channel")
      )

    val final_df = df
      .withColumnRenamed("ChannelSimple", "Channel")
      .groupBy(
        "Date", "Channel", "MarketType", "DetailedMarketType", "PredictiveClearingMode", "PredictiveClearingEnabled", "IsValuePacing", "IsManagedByTTD", "ROIGoalTypeName", "Bin_InitialBid", "Bin_FirstPriceAdjustment"
      ).agg(
        sum(col("MediaCostCPMInUSD")).alias("MediaCostCPMInUSD"),
        sum(col("PartnerCostInUSD")).alias("PartnerCostInUSD"),
        sum(col("AdvertiserCostInUSD")).alias("AdvertiserCostInUSD"),
        sum(col("FeeAmount")).alias("FeeAmount"),
        sum(col("AdjustedBidCPMInUSD")).alias("InitialBid"),
        sum(col("BidsFirstPriceAdjustment")).alias("FirstPriceAdjustment"),
        sum(col("FloorPrice")).alias("FloorPrice"),
        sum(col("FinalBidPrice")).alias("FinalBid"),
        count(when(col("IsImp"), "*")).alias("ImpressionCount"),
        count("*").alias("BidCount"),
        sum(when(
          col("FloorPrice") > 0 &&
            abs(col("FinalBidPrice") - col("FloorPrice")) < 0.001, 1).otherwise(0)
        ).alias("bidsAtFloorPlutus"),
        sum(when(
          col("IsImp") &&
            !col("BidBelowFloorExceptedSource").isin(1, 2),
          greatest(lit(0), round(col("FinalBidPrice") - col("FloorPrice"), scale = 3))).otherwise(0)
        ).alias("AvailableSurplus")
      )

    final_df.selectAs[DAPlutusDashboardSchema]
  }

  def binPriceColumn(colName: String) = {
    when(col(colName) <= 0.5, 0.5)
      .when(col(colName) > 0.5 && col(colName) <= 1, 1)
      .when(col(colName) > 1 && col(colName) <= 2.5, 2.5)
      .when(col(colName) > 2.5 && col(colName) <= 5, 5)
      .when(col(colName) > 5 && col(colName) <= 7.5, 7.5)
      .when(col(colName) > 7.5 && col(colName) <= 10, 10)
      .when(col(colName) > 10 && col(colName) <= 25, 25)
      .when(col(colName) > 25 && col(colName) <= 50, 50)
      .when(col(colName) > 50 && col(colName) <= 100, 100)
      .when(col(colName) > 100, 101)
      .otherwise(0)
  }

  def binPCColumn(colName: String) = {
    when(col(colName) <= 0.25, 0.25)
      .when(col(colName) > 0.25 && col(colName) <= 0.5, 0.5)
      .when(col(colName) > 0.5 && col(colName) <= 0.75, 0.75)
      .when(col(colName) > 0.75, 1.0)
      .otherwise(0)
  }

  def get_prov_joineddays[T <: Product : Manifest](dataset: ProvisioningS3DataSet[T], date: LocalDate, id: String): Dataset[T] = {
    // S3 provisioning takes a snapshot in the beginning of the day so adgroups that get enabled later in the day get excluded in join
    // Workaround: joining two days of S3 provisioning data to account for what's missed after the snapshot period
    val sameday = dataset.readDate(date)
    val dayafter = dataset.readDate(date.plusDays(1))

    val joined_ds = sameday.alias("same")
      .join(dayafter.alias("after"), Seq(id), "outer")

    val columns = sameday.columns.map(colName =>
      coalesce(col(s"after.$colName"), col(s"same.$colName")).alias(colName)
    )

    val final_ds = joined_ds.select(columns: _*)

    final_ds.selectAs[T]
  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    val pcResultsMergedData = loadParquetDataDailyV2[PcResultsMergedDataset](
      PcResultsMergedDataset.S3_PATH(Some(envForRead)),
      PcResultsMergedDataset.S3_PATH_DATE_GEN,
      date
    )

    val roiGoalTypeData = ROIGoalTypeDataSet().readDate(date)
    val adGroupData = get_prov_joineddays(AdGroupDataSet(), date, "AdGroupId")
    val campaignData = get_prov_joineddays(CampaignDataSet(), date, "CampaignId")

    val results_daplutusmetrics = getDAPlutusMetrics(pcResultsMergedData, roiGoalTypeData, adGroupData, campaignData)

    val outputPath = DAPlutusDashboardDataset.S3_PATH_DATE(date, envForWrite)
    results_daplutusmetrics.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
