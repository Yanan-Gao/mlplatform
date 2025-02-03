package com.thetradedesk.plutus.data.transform.dashboard

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema._
import com.thetradedesk.plutus.data.{envForRead, envForWrite, loadParquetDataDailyV2}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{SupplyVendorDataSet, SupplyVendorRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataFrameExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object PlutusDashboardDataTransform extends Logger {

  def getMarginAttribution(pcResultsMerged: Dataset[PcResultsMergedSchema],
                           supplyVendorData: Dataset[SupplyVendorRecord]
                          ): DataFrame = {

    // select necessary columns from PcResultsMerged
    val df = pcResultsMerged
      .withColumn("Date", to_date(col("LogEntryTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").cast(DateType))
      .withColumn("MarketType", when(col("DetailedMarketType") === "Open Market", lit("OM")).otherwise(lit("PMP")))
      .withColumn("hasParams", col("Mu") =!= 0.0 || col("Mu") =!= 0)
      .withColumn("hasMBTW", col("mbtw").isNotNull)
      .withColumn("hasDeal", col("DealId").isNotNull)
      .drop(col("Channel"))

    /*
    * Determine PC margin attribution based on how the FinalBid was calculated
     */

    // Calculate final bid
    val df_getFinalBid = calculateFinalBidForMarginAttribution(df, supplyVendorData)
    // Use final bid calculations for FactorCombination column (margin attribution)
    val df_addedMarginAttribution = createFactorCombination(df_getFinalBid)

    df_addedMarginAttribution
  }

  def getAggMetrics(df_addedMarginAttribution: DataFrame): Dataset[PlutusDashboardSchema] = {
    // Non-standard cases:
    // PredictiveClearingMode != 3:
    // - Model = plutus/plutusfullSample, FactorCombination = NoPlutus --> PredictiveClearingMode = 1
    // - Model = noPcApplied, FactorCombination = NoPlutus --> PredictiveClearingMode = 0
    // PC applied but metrics are null:
    // - FactorCombination = OnlyFullPlutus_NonBBF, BidsFirstPriceAdjustment != null, PCResults columns = null
    // PC applied but no mu/sigma:
    // - HasParams = false, FactorCombination != NoPlutus, PCResults columns != null

    val final_df = df_addedMarginAttribution
      .withColumnRenamed("ChannelSimple", "Channel")
      .groupBy(
        "Date", "Model", "BidBelowFloorExceptedSource", "hasParams", "hasMBTW", "hasDeal", "isMbtwValidStrict", "isMbtwValid", "Channel", "SupplyVendor", "MarketType", "DetailedMarketType", "FactorCombination"
      ).agg(
//        avg(when(col("PredictiveClearingMode") === 3, col("plutusPushdown_beforeAdj")).otherwise(null)).alias("avg_plutusPushdown"),
//        avg(col("BidsFirstPriceAdjustment")).alias("avg_FirstPriceAdjustment"),
        sum(when(col("PredictiveClearingMode") === 3, col("mode_logNorm")).otherwise(null)).alias("sum_mode_logNorm"),
        sum(when(col("PredictiveClearingMode") === 3, col("plutusPushdown_beforeAdj")).otherwise(null)).alias("sum_plutusPushdown"),
        sum(col("BidsFirstPriceAdjustment")).alias("sum_FirstPriceAdjustment"),
        sum(col("FinalBidPrice")).alias("FinalBidPrice"),
        sum(col("MediaCostCPMInUSD")).alias("MediaCostCPMInUSD"),
        sum(col("PartnerCostInUSD")).alias("PartnerCostInUSD"),
        sum(col("AdvertiserCostInUSD")).alias("AdvertiserCostInUSD"),
        sum(col("FeeAmount")).alias("FeeAmount"),
        count(when(col("IsImp"), "*")).alias("ImpressionCount"),
        count("*").alias("BidCount"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("FloorPrice") > 0 &&
            abs(col("FinalBidPrice") - col("FloorPrice")) < 0.001, 1).otherwise(0)
        ).alias("bidsAtFloorPlutus"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp") &&
            !col("BidBelowFloorExceptedSource").isin(1, 2),
          greatest(lit(0), round(col("FinalBidPrice") - col("FloorPrice"), scale = 3))).otherwise(0)
        ).alias("AvailableSurplus"),
        sum(when(
          col("PredictiveClearingMode") === 3,
          greatest(lit(0), round(col("AdjustedBidCPMInUSD") - col("FinalBidPrice"), scale = 3))).otherwise(0)
        ).alias("Surplus"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp"),
          greatest(lit(0), round(col("AdjustedBidCPMInUSD") - col("FinalBidPrice"), scale = 3))).otherwise(0)
        ).alias("Savings_FinalBid"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp"),
          greatest(lit(0), round(col("AdjustedBidCPMInUSD") - col("MediaCostCPMInUSD"), scale = 3))).otherwise(0)
        ).alias("Savings_MediaCost"),
        round(sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp") &&
            col("hasMBTW"),
          col("FinalBidPrice") - col("mbtw")).otherwise(0)), scale = 3
        ).alias("overbid_cpm"),
        round(sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp") &&
            col("hasMBTW"),
          col("FinalBidPrice")).otherwise(0)), scale = 3
        ).alias("spend_cpm"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            col("IsImp") &&
            col("hasMBTW") &&
            (col("FinalBidPrice") - col("mbtw")) > 0,
          1).otherwise(0)
        ).alias("num_overbid"),
        round(sum(when(
          col("PredictiveClearingMode") === 3 &&
            !col("IsImp") &&
            col("hasMBTW"),
          col("mbtw") - col("FinalBidPrice")).otherwise(0)), scale = 3
        ).alias("underbid_cpm"),
        round(sum(when(
          col("PredictiveClearingMode") === 3 &&
            !col("IsImp") &&
            col("hasMBTW"),
          col("FinalBidPrice")).otherwise(0)), scale = 3
        ).alias("non_spend_cpm"),
        sum(when(
          col("PredictiveClearingMode") === 3 &&
            !col("IsImp") &&
            col("hasMBTW") &&
            (col("mbtw") - col("FinalBidPrice")) > 0,
          1).otherwise(0)
        ).alias("num_underbid")
      ).withColumn("avg_plutusPushdown", lit(null).cast("Double"))
      .withColumn("avg_FirstPriceAdjustment", lit(null).cast("Double"))

    final_df.selectAs[PlutusDashboardSchema]
  }

  def calculateFinalBidForMarginAttribution(df: DataFrame, ssp: Dataset[SupplyVendorRecord]): DataFrame = {
    // Apply adjustments to Plutus Pushdown value to manually calculate FinalBid
    // Manually calculating FinalBid allows us to determine what actual factors were applied get to the FinalBid (MarginAttribution)

    // Calculate mode of log norm
    def calculateMode(mu: Double, sigma: Double): Double = {
      math.exp(mu - math.pow(sigma, 2.0))
    }

    val calculateModeUDF = udf(calculateMode _)

    // Get OpenPath SSPs
    val openPathSSPs = ssp
      .withColumn("SupplyVendor", lower(col("SupplyVendorName")))
      .select("SupplyVendor", "OpenPathEnabled")

    val newdf = df.join(broadcast(openPathSSPs), Seq("SupplyVendor"), "left")
      .withColumn("BaseBidAutoOpt", when(col("BaseBidAutoOpt") === 0, 1).otherwise(col("BaseBidAutoOpt")))
      .withColumn("prePlutusBid", col("AdjustedBidCPMInUSD") * coalesce(col("BaseBidAutoOpt"), lit(1)))
      .withColumn("GSS", col("GSS") / (col("BaseBidAutoOpt") * col("BaseBidAutoOpt")))
      .withColumn("mode_logNorm", calculateModeUDF(col("Mu").cast("Double"), col("Sigma").cast("Double")))
      .withColumn("TFPcModelBid", col("GSS") * col("prePlutusBid"))
      .withColumn("plutusPushdown_beforeAdj", col("TFPcModelBid") / col("AdjustedBidCPMInUSD"))
      // Calculate adjusted pushdown by comparing to discrepancy, openpath adjustment, and platform-wide pc pressure reducer (Strategy)
      .withColumn("EffectiveDiscrepancy", lit(1) / col("Discrepancy"))
      .withColumn("plutusPushdown_excess",
        greatest(lit(0), col("EffectiveDiscrepancy") - col("plutusPushdown_beforeAdj"))
      ).withColumn("plutusPushdown_afterAdj",
        when(col("PredictiveClearingMode").isin(0, 1), 1)
          .when(
            col("PredictiveClearingMode") === 3,
            when(col("plutusPushdown_beforeAdj") >= col("EffectiveDiscrepancy"), col("EffectiveDiscrepancy"))
              .otherwise(col("EffectiveDiscrepancy") - col("plutusPushdown_excess") * (col("Strategy") / 100))
          )
      )
      // Calculate FinalBid from adjusted pushdown and compare to floor cap
      .withColumn("proposedBid", col("AdjustedBidCPMInUSD") * coalesce(col("plutusPushdown_afterAdj"), lit(1)))
      .withColumn("calc_FinalBidPrice",
        when(col("proposedBid") > col("FloorPrice") || (col("BidBelowFloorExceptedSource").isin(1, 2) && col("PredictiveClearingMode") === 3), col("proposedBid"))
          .when((col("BidBelowFloorExceptedSource").isin(1) && col("PredictiveClearingMode") =!= 3), col("proposedBid") * coalesce(col("Discrepancy"), lit(1)))
          .otherwise(col("FloorPrice"))
      )

    newdf
  }

  def createFactorCombination(df: DataFrame): DataFrame = {
    val OpenPathAdjustment = 30
    val PressureReducer = 100

    // Determine MarginAttribution based on how the FinalBid was calculated
    df.withColumn("UseBBAO",
        when(col("BaseBidAutoOpt") =!= 1 && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseDiscrepancy",
        when(col("plutusPushdown_beforeAdj") >= col("EffectiveDiscrepancy") && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseOpenPathDiscrepancy",
        when(col("OpenPathEnabled") === true && col("Strategy") === OpenPathAdjustment && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseCampaignPCAdjustment",
        when(((col("OpenPathEnabled") === false && col("Strategy") === OpenPathAdjustment) || (!col("Strategy").isin(OpenPathAdjustment, PressureReducer))) && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseFloor",
        when(col("proposedBid") < col("FloorPrice") && !col("BidBelowFloorExceptedSource").isin(1, 2) && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseBBFGauntlet",
        when(col("BidBelowFloorExceptedSource").isin(1) && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("UseBBFPC",
        when(col("BidBelowFloorExceptedSource").isin(2) && col("PredictiveClearingMode") === 3, 1).otherwise(0)
      )
      .withColumn("NoPlutus",
        when(col("PredictiveClearingMode").isin(0, 1), 1).otherwise(0)
      )
      .withColumn("FactorCombination",
        when(col("UseBBAO") === 0 && col("UseDiscrepancy") === 0 && col("UseOpenPathDiscrepancy") === 0 && col("UseCampaignPCAdjustment") === 0 && col("UseFloor") === 0 && col("UseBBFGauntlet") === 0 && col("UseBBFPC") === 0 && col("NoPlutus") === 0, lit("OnlyFullPlutus_NonBBF"))
          .when(col("UseBBAO") === 0 && col("UseDiscrepancy") === 0 && col("UseOpenPathDiscrepancy") === 0 && col("UseCampaignPCAdjustment") === 0 && col("UseFloor") === 0 && col("UseBBFGauntlet") === 0 && col("UseBBFPC") === 0 && col("NoPlutus") === 1, "NoPlutus")
          .otherwise(concat(
            when(col("UseBBAO") === 1, lit("Bbao-")).otherwise(lit("")),
            when(col("UseDiscrepancy") === 1, lit("Discrep-")).otherwise(lit("")),
            when(col("UseOpenPathDiscrepancy") === 1, lit("OpenPath-")).otherwise(lit("")),
            when(col("UseCampaignPCAdjustment") === 1, lit("CampaignPCAdjustment-")).otherwise(lit("")),
            when(col("UseFloor") === 1, lit("Floor")).otherwise(lit("")),
            when(col("UseBBFGauntlet") === 1, lit("BBFGauntlet")).otherwise(lit("")),
            when(col("UseBBFPC") === 1, lit("BBFPC")).otherwise(lit(""))
          ))
      )
  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    val pcResultsMergedData = loadParquetDataDailyV2[PcResultsMergedSchema](
      PcResultsMergedDataset.S3_PATH(Some(envForRead)),
      PcResultsMergedDataset.S3_PATH_DATE_GEN,
      date,
      nullIfColAbsent = true
    )

    val supplyVendorData = SupplyVendorDataSet().readLatestPartitionUpTo(date)

    val df_addedMarginAttribution = getMarginAttribution(pcResultsMergedData, supplyVendorData)
    val agg_merged_df = getAggMetrics(df_addedMarginAttribution)

    val outputPath = PlutusDashboardDataset.S3_PATH_DATE(date, envForWrite)
    agg_merged_df.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
