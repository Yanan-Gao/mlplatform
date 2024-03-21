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
        count(when(col("RelevanceScore") === 0 || col("RelevanceScore") === 1, 1).otherwise(null)).as("RSMErrorCount"), // TODO: Better way to calculate
        count(when(col("RValueType") === "DynamicBid", 1).otherwise(null)).as("DynamicBidCount"),
        count(when(col("IsStarving") === true, 1).otherwise(null)).as("StarvingCount"),
        count(when(col("RValueType") === "MaxBid", 1).otherwise(null)).as("RMaxBidCount"),
        count(when(col("BidPriceBeforePredictiveClearing") === col("MaxBidCPMInAdvertiserCurrency"), 1).otherwise(null)).as("MaxBidCount")
      )
      .select(
        col("CampaignId"),
        col("AdGroupId"),
        col("ROIGoalTypeId"),
        lit(s"$reportDateStr $hour:00:00").as("ReportHourUtc"),
        col("TotalCount"),
        to_json(struct(col("RSMErrorCount"), col("DynamicBidCount"), col("StarvingCount"), col("RMaxBidCount"), col("MaxBidCount"))).as("CountMetrics")
      )
      .as[DiagnosisRecord]
  }

  def runETLPipeline(): Unit = {
    val date = dateTime.toLocalDate
    val hour = dateTime.getHour

    DiagnosisDataset().writePartition(
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