package com.thetradedesk.audience.jobs

import com.thetradedesk.audience.datasets._
import com.thetradedesk.audience.dateTime
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.AdGroupDataSet
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DiagnosisDataGenerator {

  def getDiagnosisData(date: LocalDate, hour: Int): Dataset[DiagnosisRecord] = {
    val dateStr = date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val reportDateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    val campaignpacing = CampaignPacingSettingsDataset().readLatestPartition()

    val adgroups = AdGroupDataSet().readLatestPartition()
      .select("CampaignId", "AdGroupId", "ROIGoalTypeId", "MaxBidCPMInAdvertiserCurrency")
      .join(campaignpacing, Seq("CampaignId"), "left")
      .filter(col("IsValuePacing") === true)

    val auctionlog = spark.read.parquet(s"s3a://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date=$dateStr/hour=$hour")
      .select(col("AvailableBidRequestId"), explode(col("AdGroupCandidates")))
      .select(col("AvailableBidRequestId"), col("col.*"))

    auctionlog.join(adgroups, Seq("AdGroupId"))
      .groupBy("CampaignId", "AdGroupId", "ROIGoalTypeId")
      .agg(
        count("*").as("TotalCount"),
        count(when(col("IsRelevanceResultSuccess") === false, 1).otherwise(null)).as("RSMErrorCount"),
        count(when(col("IsStarving") === true, 1).otherwise(null)).as("StarvingCount"),
        count(when(col("RValueType") === "DynamicBid", 1).otherwise(null)).as("DynamicBidCount"),
        count(when(col("RValueType") === "ControlledBid", 1).otherwise(null)).as("ControlledBidCount"),
        count(when(col("RValueType") === "MaxBid", 1).otherwise(null)).as("RMaxBidCount"),
        count(when(col("BidPriceBeforePredictiveClearing") === col("MaxBidCPMInAdvertiserCurrency"), 1).otherwise(null)).as("MaxBidCount"),
        count(when(col("MatchedPrivateContract") === true, 1).otherwise(null)).as("PrivateContractCount"),
        count(when(col("RelevanceResultSource") === 0, 1).otherwise(null)).as("RelevanceSourceNoneCount"),
        count(when(col("RelevanceResultSource") === 1, 1).otherwise(null)).as("RelevanceSourceModelCount"),
        count(when(col("RelevanceResultSource") === 2, 1).otherwise(null)).as("RelevanceSourceDefaultCount"),
        count(when(col("RelevanceResultSource") === 3, 1).otherwise(null)).as("RelevanceSourceNoSeedCount"),
        count(when(col("RelevanceResultSource") === 4, 1).otherwise(null)).as("RelevanceSourceNoSyntheticIdCount"),
        count(when(col("RelevanceResultSource") === 5, 1).otherwise(null)).as("RelevanceSourceNoEmbeddingCount"),
        sum(col("ExpectedValue")).as("ExpectedValueSum"),
        sum(col("ExpectedValue") * col("ExpectedValue")).as("ExpectedValueSquaredSum"),
        sum(col("RelevanceScore")).as("RelevanceScoreSum"),
        sum(col("RelevanceScore") * col("RelevanceScore")).as("RelevanceScoreSquaredSum"),
        sum(col("KpiModelOutput")).as("KpiModelOutputSum"),
        sum(col("KpiModelOutput") * col("KpiModelOutput")).as("KpiModelOutputSquaredSum"),
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
      )
      .select(
        col("CampaignId"),
        col("AdGroupId"),
        col("ROIGoalTypeId"),
        lit(s"$reportDateStr $hour:00:00").as("ReportHourUtc"),
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
          col("UserBaseBidFactorSquaredSum")
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