package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets.FloorAgnosticBiddingMetricsDataset.{MAXBID_MULTIPLIER_DEFAULT, PC_BACKOFF_NO_ADJUSTMENT, PC_BACKOFF_NO_OPTOUT, PC_PLATFORM_BBF_BUFFER}
import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.dateTime
import com.thetradedesk.geronimo.shared.loadParquetData
import com.thetradedesk.spark.datasets.sources.AdvertiserDataSet
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdGroupDataSet
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DiagnosisDataGenerator {

  def getDiagnosisData(date: LocalDate, hour: Int): Dataset[DiagnosisRecord] = {
    val dateStr = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val reportDateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val advertisers = AdvertiserDataSet().readLatestPartitionUpTo(date, true)
      .select("AdvertiserId", "CurrencyCodeId")

    val exchangeRate = spark.read.parquet(s"s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/currencyexchangerate/v=1/date=$dateStr/")
      .withColumn("row", row_number().over(Window.partitionBy("CurrencyCodeId").orderBy(desc("AsOfDateUTC"))))
      .filter(col("row") === 1)
      .select("CurrencyCodeId", "FromUSD")

    // FAB (Floor Agnostic Bidding) metrics
    val fabMetrics = loadParquetData[FloorAgnosticBiddingMetricsRecord](s3path = FloorAgnosticBiddingMetricsDataset.PCMetricsS3, date = date, lookBack = Some(5), getLatestDateOnly = Some(true))

    val adgroups = AdGroupDataSet().readLatestPartitionUpTo(date, true)
    .join(advertisers, Seq("AdvertiserId"), "left")
    .join(exchangeRate, Seq("CurrencyCodeId"), "left")
    .withColumn("MaxBidCPM", col("MaxBidCPMInAdvertiserCurrency") / col("FromUSD"))
    .select("CampaignId", "AdGroupId", "ROIGoalTypeId", "MaxBidCPM")

    val auctionlog = spark.read.parquet(s"s3a://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date=$dateStr/hour=$hour")
      .select(col("AvailableBidRequestId"), explode(col("AdGroupCandidates")))
      .select(col("AvailableBidRequestId"), col("col.*"))
      .filter(col("IsValuePacing") === true)

    auctionlog.join(adgroups, Seq("AdGroupId", "CampaignId"), "inner")
      .groupBy("CampaignId", "AdGroupId", "ROIGoalTypeId")
      .agg(
        count("*").as("TotalCount"),
        count(when(col("IsRelevanceResultSuccess") === false, 1).otherwise(null)).as("RSMErrorCount"),
        count(when(col("IsStarving") === true, 1).otherwise(null)).as("StarvingCount"),
        count(when(col("RValueType") === "DynamicBid", 1).otherwise(null)).as("DynamicBidCount"),
        count(when(col("RValueType") === "ControlledBid", 1).otherwise(null)).as("ControlledBidCount"),
        count(when(col("RValueType") === "MaxBid", 1).otherwise(null)).as("RMaxBidCount"),
        count(when(col("BidPriceBeforePredictiveClearing") >= col("MaxBidCPM") * 0.99, 1).otherwise(null)).as("MaxBidCount"),
        count(when(col("MatchedPrivateContract") === true, 1).otherwise(null)).as("PrivateContractCount"),
        count(when(col("RelevanceResultSource") === 0, 1).otherwise(null)).as("RelevanceSourceNoneCount"),
        count(when(col("RelevanceResultSource") === 1, 1).otherwise(null)).as("RelevanceSourceModelCount"),
        count(when(col("RelevanceResultSource").isin(2, 6, 7, 8, 9), 1).otherwise(null)).as("RelevanceSourceDefaultCount"),
        count(when(col("RelevanceResultSource") === 3, 1).otherwise(null)).as("RelevanceSourceNoSeedCount"),
        count(when(col("RelevanceResultSource") === 4, 1).otherwise(null)).as("RelevanceSourceNoSyntheticIdCount"),
        count(when(col("RelevanceResultSource") === 5, 1).otherwise(null)).as("RelevanceSourceNoEmbeddingCount"),
        sum(col("ExpectedValue")).as("ExpectedValueSum"),
        sum(col("ExpectedValue") * col("ExpectedValue")).as("ExpectedValueSquaredSum"),
        sum(col("RelevanceScore")).as("RelevanceScoreSum"),
        sum(col("RelevanceScore") * col("RelevanceScore")).as("RelevanceScoreSquaredSum"),
        sum(col("KpiModelOutput")).as("KpiModelOutputSum"),
        sum(col("KpiModelOutput") * col("KpiModelOutput")).as("KpiModelOutputSquaredSum"),
        sum(when(col("RValueType") === "DynamicBid", col("KpiModelOutput")).otherwise(0)).as("KpiModelOutputSum2"),
        sum(when(col("RValueType") === "DynamicBid", col("KpiModelOutput") * col("KpiModelOutput")).otherwise(0)).as("KpiModelOutputSquaredSum2"),
        sum(col("PriorityBidFactor")).as("PriorityBidFactorSum"),
        sum(col("PriorityBidFactor") * col("PriorityBidFactor")).as("PriorityBidFactorSquaredSum"),
        sum(col("FrequencyBidFactor")).as("FrequencyBidFactorSum"),
        sum(col("FrequencyBidFactor") * col("FrequencyBidFactor")).as("FrequencyBidFactorSquaredSum"),
        sum(col("VolumeControlBidFactor")).as("VolumeControlBidFactorSum"),
        sum(col("VolumeControlBidFactor") * col("VolumeControlBidFactor")).as("VolumeControlBidFactorSquaredSum"),
        sum(col("TraderBidFactor")).as("TraderBidFactorSum"),
        sum(col("TraderBidFactor") * col("TraderBidFactor")).as("TraderBidFactorSquaredSum"),
        sum(col("HmrmfpBidFactor")).as("HmrmfpBidFactorSum"),
        sum(col("HmrmfpBidFactor") * col("HmrmfpBidFactor")).as("HmrmfpBidFactorSquaredSum"),
        sum(col("UserBaseBidFactor")).as("UserBaseBidFactorSum"),
        sum(col("UserBaseBidFactor") * col("UserBaseBidFactor")).as("UserBaseBidFactorSquaredSum"),
        sum(col("RValueUsed")).as("RValueUsedSum"),
        sum(col("RValueUsed") * col("RValueUsed")).as("RValueUsedSquaredSum"),
        sum(col("SystemAutoBidFactor")).as("SystemAutoBidFactorSum"),
        sum(col("SystemAutoBidFactor") * col("SystemAutoBidFactor")).as("SystemAutoBidFactorSquaredSum"),
        sum(when(col("RValueType") === "DynamicBid", col("RawKpiModelOutput")).otherwise(0)).as("RawKpiModelOutputSum"),
        sum(when(col("RValueType") === "DynamicBid", col("PerformanceModelScoreBeforeApplyingChangeRatio")).otherwise(0)).as("PerformanceModelScoreBeforeApplyingChangeRatioSum"),
        sum(col("RelevanceScoreAfterVersionAdjustment")).as("RelevanceScoreAfterVersionAdjustmentSum"),
        sum(col("AdjustedRelevanceScore")).as("AdjustedRelevanceScoreSum"),
        max(col("ModelVersion")).as("ModelVersionMax"),
        max(col("RelevanceModelVersion")).as("RelevanceModelVersionMax"),
        corr(col("ExpectedValue"), col("RelevanceScore")).as("CorrEVxRSM"),
        corr(col("ExpectedValue"), col("KpiModelOutput")).as("CorrEVxKPI"),
        corr(col("ExpectedValue"), col("RValueUsed")).as("CorrEVxR"),
        corr(col("ExpectedValue"), col("FrequencyBidFactor")).as("CorrEVxFrequency"),
        corr(col("ExpectedValue"), col("VolumeControlBidFactor")).as("CorrEVxVC"),
        corr(col("ExpectedValue"), col("TraderBidFactor")).as("CorrEVxTrader"),
        corr(col("ExpectedValue"), col("UserBaseBidFactor")).as("CorrEVxUserBase"),
        corr(col("ExpectedValue"), col("SystemAutoBidFactor")).as("CorrEVxSystemAuto"),
        corr(col("RelevanceScore"), col("KpiModelOutput")).as("CorrRSMxKPI"),
        corr(col("RelevanceScore"), col("RValueUsed")).as("CorrRSMxR"),
        corr(col("RelevanceScore"), col("FrequencyBidFactor")).as("CorrRSMxFrequency"),
        corr(col("RelevanceScore"), col("VolumeControlBidFactor")).as("CorrRSMxVC"),
        corr(col("RelevanceScore"), col("TraderBidFactor")).as("CorrRSMxTrader"),
        corr(col("RelevanceScore"), col("UserBaseBidFactor")).as("CorrRSMxUserBase"),
        corr(col("RelevanceScore"), col("SystemAutoBidFactor")).as("CorrRSMxSystemAuto"),
        corr(col("KpiModelOutput"), col("RValueUsed")).as("CorrKPIxR"),
        corr(col("KpiModelOutput"), col("FrequencyBidFactor")).as("CorrKPIxFrequency"),
        corr(col("KpiModelOutput"), col("VolumeControlBidFactor")).as("CorrKPIxVC"),
        corr(col("KpiModelOutput"), col("TraderBidFactor")).as("CorrKPIxTrader"),
        corr(col("KpiModelOutput"), col("UserBaseBidFactor")).as("CorrKPIxUserBase"),
        corr(col("KpiModelOutput"), col("SystemAutoBidFactor")).as("CorrKPIxSystemAuto"),
        corr(col("RValueUsed"), col("FrequencyBidFactor")).as("CorrRxFrequency"),
        corr(col("RValueUsed"), col("VolumeControlBidFactor")).as("CorrRxVC"),
        corr(col("RValueUsed"), col("TraderBidFactor")).as("CorrRxTrader"),
        corr(col("RValueUsed"), col("UserBaseBidFactor")).as("CorrRxUserBase"),
        corr(col("RValueUsed"), col("SystemAutoBidFactor")).as("CorrRxSystemAuto"),
      )
      .join(fabMetrics, Seq("CampaignId"), "left")
      .withColumn("CampaignPCBackoffAdjustment", coalesce(col("CampaignPCAdjustment"), lit(PC_BACKOFF_NO_ADJUSTMENT)))
      .withColumn("CampaignFabFloorBuffer", coalesce(col("CampaignBbfFloorBuffer"), lit(PC_PLATFORM_BBF_BUFFER)))
      .withColumn("CampaignFabOptOutRate", coalesce(col("Actual_BBF_OptOut_Rate"), lit(PC_BACKOFF_NO_OPTOUT)))
      .withColumn("MaxBidMultiplierCap", coalesce(col("MaxBidMultiplierCap"), lit(MAXBID_MULTIPLIER_DEFAULT)))
      .select(
        col("CampaignId"),
        col("AdGroupId"),
        col("ROIGoalTypeId"),
        to_timestamp(lit(s"$reportDateStr $hour:00:00"), "yyyy-MM-dd H:mm:ss").as("ReportHourUtc"),
        col("TotalCount"),
        to_json(struct(
          col("RSMErrorCount"),
          col("StarvingCount"),
          col("DynamicBidCount"),
          col("ControlledBidCount"),
          col("RMaxBidCount"),
          col("MaxBidCount"),
          col("PrivateContractCount"),
          col("RelevanceSourceNoneCount"),
          col("RelevanceSourceModelCount"),
          col("RelevanceSourceDefaultCount"),
          col("RelevanceSourceNoSeedCount"),
          col("RelevanceSourceNoSyntheticIdCount"),
          col("RelevanceSourceNoEmbeddingCount"),
          col("ExpectedValueSum"),
          col("ExpectedValueSquaredSum"),
          col("RelevanceScoreSum"),
          col("RelevanceScoreSquaredSum"),
          col("KpiModelOutputSum"),
          col("KpiModelOutputSquaredSum"),
          col("KpiModelOutputSum2"),
          col("KpiModelOutputSquaredSum2"),
          col("PriorityBidFactorSum"),
          col("PriorityBidFactorSquaredSum"),
          col("FrequencyBidFactorSum"),
          col("FrequencyBidFactorSquaredSum"),
          col("VolumeControlBidFactorSum"),
          col("VolumeControlBidFactorSquaredSum"),
          col("TraderBidFactorSum"),
          col("TraderBidFactorSquaredSum"),
          col("HmrmfpBidFactorSum"),
          col("HmrmfpBidFactorSquaredSum"),
          col("UserBaseBidFactorSum"),
          col("UserBaseBidFactorSquaredSum"),
          col("RValueUsedSum"),
          col("RValueUsedSquaredSum"),
          col("SystemAutoBidFactorSum"),
          col("SystemAutoBidFactorSquaredSum"),
          col("RawKpiModelOutputSum"),
          col("PerformanceModelScoreBeforeApplyingChangeRatioSum"),
          col("RelevanceScoreAfterVersionAdjustmentSum"),
          col("AdjustedRelevanceScoreSum"),
          col("ModelVersionMax"),
          col("RelevanceModelVersionMax"),
          col("CorrEVxRSM"),
          col("CorrEVxKPI"),
          col("CorrEVxR"),
          col("CorrEVxFrequency"),
          col("CorrEVxVC"),
          col("CorrEVxTrader"),
          col("CorrEVxUserBase"),
          col("CorrEVxSystemAuto"),
          col("CorrRSMxKPI"),
          col("CorrRSMxR"),
          col("CorrRSMxFrequency"),
          col("CorrRSMxVC"),
          col("CorrRSMxTrader"),
          col("CorrRSMxUserBase"),
          col("CorrRSMxSystemAuto"),
          col("CorrKPIxR"),
          col("CorrKPIxFrequency"),
          col("CorrKPIxVC"),
          col("CorrKPIxTrader"),
          col("CorrKPIxUserBase"),
          col("CorrKPIxSystemAuto"),
          col("CorrRxFrequency"),
          col("CorrRxVC"),
          col("CorrRxTrader"),
          col("CorrRxUserBase"),
          col("CorrRxSystemAuto"),
          col("CampaignPCBackoffAdjustment"),
          col("CampaignFabFloorBuffer"),
          col("CampaignFabOptOutRate"),
          col("MaxBidMultiplierCap")
        )).as("CountMetrics")
      )
      .as[DiagnosisRecord]
  }

  def runETLPipeline(): Unit = {
    val date = dateTime.toLocalDate
    val hour = dateTime.getHour

    DiagnosisWritableDataset().writePartition(
      dataset = getDiagnosisData(date, hour),
      partition = date,
      subFolderKey = Some("hour"),
      subFolderValue = Some("%02d".format(hour))
    )
  }

  def main(args: Array[String]): Unit = {
    runETLPipeline()
  }
}