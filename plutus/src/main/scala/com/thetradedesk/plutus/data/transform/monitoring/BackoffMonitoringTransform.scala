package com.thetradedesk.plutus.data.transform.monitoring

import com.thetradedesk.plutus.data.{AuctionType, envForRead}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData, PlutusOptoutBidsDataset}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignThrottleMetricDataset, CampaignThrottleMetricSchema, MergedCampaignAdjustmentsDataset}
import com.thetradedesk.plutus.data.schema.monitoring.{BackoffMonitoringDataset, BackoffMonitoringSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{bucketCount, getTestBucketUDF, handleDuplicateCampaignFlights}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

import java.time.LocalDate

object BackoffMonitoringTransform {

  val underdeliveryFraction = 0.05

  case class RelevantBidData(
                              CampaignId: String,
                              AdGroupId: String,
                              PredictiveClearingEnabled: Option[Boolean],
                              PredictiveClearingApplied: Option[Boolean],
                              IsValuePacing: Boolean,
                              Model: String,
                              BidBelowFloorExceptedSource: Int,
                              AuctionType: Option[Int],
                              DealId: String,
                              MaxBidMultiplierCap: Double,
                              MaxBidCpmInBucks: Double,
                              InitialBid: Double,
                              MediaCostCPMInUSD: Option[Double],
                              FeeAmount: Option[Double],
                              PartnerCostInUSD: Option[Double],
                              IsRealBid: Boolean
                            )

  case class AggregatedCampaignBidOptOutMetrics(
                              CampaignId: String,
                              IsValuePacing: Boolean,
                              PCBid_Rate: Option[Double],
                              PCSpend_Rate: Option[Double],
                              MaxBidMultiplierCap: Double,
                              InitialBid: Option[Double],
                              MaxBidCpmInBucks: Option[Double],
                              InternalBidOverMaxBid_Rate: Option[Double],
                              FeeAmount: Option[Double],
                              PartnerCostInUSD: Option[Double],
                              PCFeesOverSpend_Rate: Option[Double],
                              // Calculated OptOut Rates
                              BBF_OptOut_Rate: Option[Double],
                              BBF_PMP_OptOut_Rate: Option[Double],
                              BBF_OM_OptOut_Rate: Option[Double],
                              NonBBF_OptOut_Rate: Option[Double],
                              // Bid Counts
                              Bids: Option[Long],
                              PMP_Bids: Option[Long],
                              OM_Bids: Option[Long],
                              BBF_OptOut_Bids: Option[Long],
                              BBF_PMP_OptOut_Bids: Option[Long],
                              BBF_OM_OptOut_Bids: Option[Long],
                              NonBBF_OptOut_Bids: Option[Long]
                            )

  case class CampaignUnderdeliveryData(
                                        CampaignId: String,
                                        IsValuePacing: Boolean,
                                        IsBaseBidOptimized: String,
                                        IsProgrammaticGuaranteed: String,
                                        UnderdeliveryFraction: Double,
                                        TotalAdvertiserCostFromPerformanceReportInUSD: Double,
                                        EstimatedBudgetInUSD: Double
                                      )

  case class CampaignBackoffData(
                                  CampaignId: String,
                                  Sim_BBF_OptOut_Rate: Option[Double],
                                  Sim_BBF_PMP_OptOut_Rate: Option[Double],
                                  Sim_BBF_OM_OptOut_Rate: Option[Double],
                                  CampaignBbfFloorBuffer: Option[Double],
                                  CampaignPCAdjustment: Option[Double],
                                  VirtualMaxBid_Multiplier: Option[Double]
                                )

  def unionBidAndOptOutData(
                               adGroupData: Dataset[AdGroupRecord],
                               pcOptOutData: Dataset[PlutusLogsData],
                               pcBidData: Dataset[PcResultsMergedSchema]
                             ): Dataset[RelevantBidData] = {

    val relevantPcOptOutData = pcOptOutData
      .join(broadcast(adGroupData.select("AdgroupId", "CampaignId", "PredictiveClearingEnabled").distinct()), Seq("AdgroupId"), "inner")
      // Using bid level signals to determine if PC is enabled for the campaign.
      .withColumn("PredictiveClearingApplied", // OptOut dataset only has Model column, so it needs to be handled differently than Bid dataset.
        when(col("Model") === "noPcApplied", false)
          .when(col("Model") =!= "noPcApplied" && col("Model").isNotNull, true)
          .otherwise(null)// If Model = null, we can't determine if PC being used or not without other signals like PredictiveClearingEnabled.
      )
      .withColumn("MediaCostCPMInUSD", lit(0.0).cast("Double"))
      .withColumn("FeeAmount", lit(0.0).cast("Double"))
      .withColumn("PartnerCostInUSD", lit(0.0).cast("Double"))
      .withColumn("IsImp", lit(null).cast("Boolean"))
      .withColumn("IsRealBid", lit(false))
      .selectAs[RelevantBidData]

    val relevantPcBidData = pcBidData
      .join(broadcast(adGroupData.select("AdgroupId", "PredictiveClearingEnabled").distinct()), Seq("AdgroupId"), "left")
      // Using bid level signals to determine if PC is enabled for the campaign.
      .withColumn("PredictiveClearingApplied", // Bid dataset can use both PCMode & Model columns to determine value.
        when(col("PredictiveClearingMode") === 0 && (col("Model") === "noPcApplied" || col("Model").isNull), false)
          .when(col("PredictiveClearingMode").isin(1,3) && (col("Model") =!= "noPcApplied" || col("Model").isNull), true)
          .otherwise(null)
      )
      .withColumn("IsRealBid", lit(true))
      .selectAs[RelevantBidData]

    relevantPcOptOutData.union(relevantPcBidData)
  }

  def aggregateBidOptOutData(
                                totalBidData: Dataset[RelevantBidData]
                              ): Dataset[AggregatedCampaignBidOptOutMetrics] = {
    totalBidData
      .withColumn("Market",
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket")
      )
      .withColumn("PredictiveClearingEnabled_Status", coalesce(col("PredictiveClearingEnabled"), col("PredictiveClearingApplied")))
      .groupBy("CampaignId")
      .agg(
        max(col("IsValuePacing")).as("IsValuePacing"),
        max(col("MaxBidMultiplierCap")).as("MaxBidMultiplierCap"),
        // Bid Data
        sum(
          when(col("IsRealBid"), lit(1))
            .otherwise(0)
        ).as("Bids"),
        sum(
          when(col("IsRealBid") && col("Market").isin("Variable", "Fixed"), lit(1))
            .otherwise(lit(0))
        ).as("PMP_Bids"),
        sum(
          when(col("IsRealBid") && col("Market") === "OpenMarket", lit(1)).otherwise(lit(0))
        ).as("OM_Bids"),
        // OptOut Bid Data
        sum(
          when(!col("IsRealBid") && col("BidBelowFloorExceptedSource") === 2, lit(1))
            .otherwise(0)
        ).as("BBF_OptOut_Bids"),
        sum(
          when(!col("IsRealBid") && col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed"), lit(1))
            .otherwise(lit(0))
        ).as("BBF_PMP_OptOut_Bids"),
        sum(
          when(!col("IsRealBid") && col("BidBelowFloorExceptedSource") === 2 && col("Market") === "OpenMarket", lit(1)).otherwise(lit(0))
        ).as("BBF_OM_OptOut_Bids"),
        sum(
          when(!col("IsRealBid") && col("BidBelowFloorExceptedSource") =!= 2, lit(1))
            .otherwise(0)
        ).as("NonBBF_OptOut_Bids"),
        // Combined
        sum(
          when(col("PredictiveClearingEnabled_Status"), lit(1))
            .otherwise(0)
        ).as("PC_TotalBids"),
        sum(
          when(col("PredictiveClearingEnabled_Status"), $"MediaCostCPMInUSD" * lit(1000))
            .otherwise(0)
        ).as("PC_Spend"),
        sum($"MediaCostCPMInUSD" * lit(1000)).as("Spend"),
        sum("InitialBid").as("InitialBid"),
        sum("MaxBidCpmInBucks").as("MaxBidCpmInBucks"),
        sum("FeeAmount").as("FeeAmount"),
        sum("PartnerCostInUSD").as("PartnerCostInUSD")
      )
      .withColumn("BBF_OptOut_Rate", col("BBF_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      .withColumn("BBF_PMP_OptOut_Rate", col("BBF_PMP_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      .withColumn("BBF_OM_OptOut_Rate", col("BBF_OM_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      // Following OptOut Rate does not include BBF Opt Out
      .withColumn("NonBBF_OptOut_Rate", col("NonBBF_OptOut_Bids") / (col("Bids") + col("NonBBF_OptOut_Bids")))
      // Get MaxBid & Fee Rates
      .withColumn("InternalBidOverMaxBid_Rate", col("InitialBid") / col("MaxBidCpmInBucks"))
      .withColumn("PCFeesOverSpend_Rate", col("FeeAmount") / col("PartnerCostInUSD")) // This is PC margin, just not in basis points
      // Get PC Rates
      .withColumn("PCBid_Rate", col("PC_TotalBids") / (col("Bids") + col("BBF_OptOut_Bids") + col("NonBBF_OptOut_Bids")))
      .withColumn("PCSpend_Rate", col("PC_Spend") / col("Spend"))
      .selectAs[AggregatedCampaignBidOptOutMetrics]
  }

  def getBackoffMetrics(backOffData: DataFrame): Dataset[CampaignBackoffData] = {
    backOffData
      .withColumn("Sim_BBF_BidCount", $"hdv3_BBF_PMP_BidCount" + $"hdv3_BBF_OM_BidCount")
      // hdv3_Total_BidCount is sum of BBF Opt Out Bids and Bids
      .withColumn("Sim_BBF_OptOut_Rate", $"Sim_BBF_BidCount" / $"hdv3_Total_BidCount")
      .withColumn("Sim_BBF_PMP_OptOut_Rate", $"hdv3_BBF_PMP_BidCount" / $"hdv3_Total_BidCount")
      .withColumn("Sim_BBF_OM_OptOut_Rate", $"hdv3_BBF_OM_BidCount" / $"hdv3_Total_BidCount")
      .withColumnRenamed("MergedPCAdjustment", "CampaignPCAdjustment")
      .selectAs[CampaignBackoffData]
  }

  def getThrottleMetrics(campaignThrottleData: Dataset[CampaignThrottleMetricSchema]): Dataset[CampaignUnderdeliveryData] = {
    campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        first($"IsValuePacing").as("IsValuePacing"),
        first($"IsBaseBidOptimized").as("IsBaseBidOptimized"), // Anchor Mode
        first($"IsProgrammaticGuaranteed").as("IsProgrammaticGuaranteed"), // PG
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction"),
        max($"TotalAdvertiserCostFromPerformanceReportInUSD").as("TotalAdvertiserCostFromPerformanceReportInUSD"),
        max($"EstimatedBudgetInUSD").as("EstimatedBudgetInUSD"),
      ).selectAs[CampaignUnderdeliveryData]
  }

  def combineMetrics(
                      date: LocalDate,
                      backoffMetrics: Dataset[CampaignBackoffData],
                      bidOptOutMetrics: Dataset[AggregatedCampaignBidOptOutMetrics],
                      campaignThrottleData: Dataset[CampaignThrottleMetricSchema]
                    ): Dataset[BackoffMonitoringSchema] = {
    val campaignUnderdeliveryData = getThrottleMetrics(campaignThrottleData)

    bidOptOutMetrics
      .withColumnRenamed("IsValuePacing", "bom_IsValuePacing")
      .join(broadcast(campaignUnderdeliveryData.withColumnRenamed("IsValuePacing", "ud_IsValuePacing")), Seq("CampaignId"), "left")
      .withColumn("IsValuePacing", coalesce(col("bom_IsValuePacing"), col("ud_IsValuePacing")))
      .drop("bom_IsValuePacing", "ud_IsValuePacing")
      .join(broadcast(backoffMetrics), Seq("CampaignId"), "left")
      .withColumn("CampaignPredictiveClearingEnabled", when(col("PCBid_Rate") >= lit(0.5) || col("PCSpend_Rate") >= lit(0.5), true).otherwise(false))
      // Get Test Buckets
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount).cast("int")))
      .withColumn(
        "BucketGroup",
        when(col("IsValuePacing"),
          when(col("TestBucket").between(0, 899), "Hades Test Group")
            .when(col("TestBucket").between(900, 1000), "Hades Control Group")
            .otherwise("Out of defined range")
        ).when(!col("IsValuePacing"), "Solimar")
          .otherwise(null)
      )
      .withColumn("date", lit(date))
      .selectAs[BackoffMonitoringSchema]
  }

  // Not sure yet if labeling will be done during metric generation or manually during analysis.
  // Leaving this code here in case we decide to use it later.
  //  def computeCampaignThresholds(
//                       df: DataFrame
//                     ): (Double, Double, Double) = {
//
//    val row: Row = df
//      .filter($"InternalBidOverMaxBid_Rate" <= 1 && $"IsValuePacing" && $"IsBaseBidOptimized" != "AllAnchored" && $"IsProgrammaticGuaranteed" != "PG")
//      .agg(
//        expr(s"percentile_approx(UnderdeliveryFraction, 0.75)").alias("Q3_UnderdeliveryFraction"),
//        expr(s"percentile_approx(InternalBidOverMaxBid_Rate, 0.75)").alias("Q3_InternalBidOverMaxBid_Ratio"),
//        expr(s"percentile_approx(PCFeesOverSpend_Rate, 0.75)").alias("Q3_PCFeesOverSpend_Ratio"),
//      )
//      .head()
//    (
//      row.getAs[Double]("Q3_UnderdeliveryFraction"),
//      row.getAs[Double]("Q3_InternalBidOverMaxBid_Ratio"),
//      row.getAs[Double]("Q3_PCFeesOverSpend_Ratio")
//    )
//  }
//
//  val PacingStatus_NoPacingData = "NoPacingData"
//  val PacingStatus_NotPacing_HighUnderdelivery = "NotPacing_HighUnderdelivery"
//  val PacingStatus_NotPacing_NonHighUnderdelivery = "NotPacing_NonHighUnderdelivery"
//  val PacingStatus_Pacing = "Pacing"
//
//  val BidsAtMaxBidStatus_NoData = "NoBidsAtMaxBid"
//  val BidsAtMaxBidStatus_High = "HighBidsAtMaxBid"
//  val BidsAtMaxBidStatus_NotHigh = "NotHighBidsAtMaxBid"
//
//  val PCFeesStatus_NoFees = "NoPCFees"
//  val PCFeesStatus_HighFees = "HighPCFees"
//  val PCFeesStatus_NotHighFees = "NotHighPCFees"
//
//  val CombinedPacingPCFeesStatus_HighUnderdeliveryAndFees = "HighUnderdeliveryAndPCFees"
//  val CombinedPacingPCFeesStatus_OnlyHighUnderdelivery = "OnlyHighUnderdelivery"
//  val CombinedPacingPCFeesStatus_OnlyHighFees = "OnlyHighPCFees"
//  val CombinedPacingPCFeesStatus_Neither = "Neither"
//
//  def identifyProblemCampaigns(
//                                date: LocalDate,
//                                hadesBufferBackoffMetrics: DataFrame,
//                                bidOptOutMetrics: DataFrame,
//                                finalCampaignThrottleData: Dataset[CampaignThrottleMetricSchema]
//                              ): Dataset[BackoffMonitoringSchema] = {
//    val res = combineMetrics(date, hadesBufferBackoffMetrics, bidOptOutMetrics, finalCampaignThrottleData, campaignDAPlutusDashboardData)
//
//    val (highUnderdeliveryFraction_Threshold, highInternalBidOverMaxBid_Rate_Threshold, highPCFeesOverSpend_Rate_Threshold) =
//      computeCampaignThresholds(res)
//
//    // Should we also weight campaigns by UDF to determine if they're a "problem" campaign"
//    val labelCampaigns = res
//      .withColumn("PacingStatus",
//        when($"UnderdeliveryFraction".isNull, PacingStatus_NoPacingData
//        ).when($"UnderdeliveryFraction" > underdeliveryThreshold,
//            when($"UnderdeliveryFraction" > highUnderdeliveryFraction_Threshold, PacingStatus_NotPacing_HighUnderdelivery)
//              .otherwise(PacingStatus_NotPacing_NonHighUnderdelivery)
//        ).otherwise(PacingStatus_Pacing))
//      .withColumn("InternalBidOverMaxBidStatus",
//        when($"InternalBidOverMaxBid_Rate".isNull || $"InternalBidOverMaxBid_Rate" === 0.0, BidsAtMaxBidStatus_NoData)
//          .when($"InternalBidOverMaxBid_Rate" > highInternalBidOverMaxBid_Rate_Threshold, BidsAtMaxBidStatus_High)
//          .otherwise(BidsAtMaxBidStatus_NotHigh))
//      .withColumn("PCFeesStatus",
//        when($"PCFeesOverSpend_Rate".isNull || $"PCFeesOverSpend_Rate" === 0.0, PCFeesStatus_NoFees)
//          .when($"PCFeesOverSpend_Rate" > highPCFeesOverSpend_Rate_Threshold, PCFeesStatus_HighFees)
//          .otherwise(PCFeesStatus_NotHighFees))
//      .withColumn("ProblemStatus",
//        when($"PacingStatus" === PacingStatus_NotPacing_HighUnderdelivery && $"InternalBidOverMaxBidStatus" === BidsAtMaxBidStatus_High,
//          when($"PCFeesOverSpend_Rate" === PCFeesStatus_HighFees, CombinedPacingPCFeesStatus_HighUnderdeliveryAndFees)
//            .otherwise(CombinedPacingPCFeesStatus_OnlyHighUnderdelivery)
//        ).when($"PCFeesOverSpend_Rate" === PCFeesStatus_HighFees, CombinedPacingPCFeesStatus_OnlyHighFees
//        ).otherwise(CombinedPacingPCFeesStatus_Neither))
//      .withColumn("HighUnderdeliveryFraction_Threshold", lit(highUnderdeliveryFraction_Threshold))
//      .withColumn("HighInternalBidOverMaxBid_Rate_Threshold", lit(highInternalBidOverMaxBid_Rate_Threshold))
//      .withColumn("HighPCFeesOverSpend_Rate_Threshold", lit(highPCFeesOverSpend_Rate_Threshold))
//      .selectAs[BackoffMonitoringSchema]
//  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    // Get metrics from the buffer backoff job data
    val backoffData = MergedCampaignAdjustmentsDataset.readDataframe(date, env = envForRead)
    val backoffMetrics = getBackoffMetrics(backoffData)

    // Get metrics from throttle dataset
    val campaignThrottleData = CampaignThrottleMetricDataset.readDate(env = envForRead, date = date)
    val finalCampaignThrottleData = handleDuplicateCampaignFlights(campaignThrottleData)

    // Calculate real optout metrics:
    // Step 1 : Read PC Optout data
    val pcOptOutData = PlutusOptoutBidsDataset.readDate(date, env = envForRead, nullIfColAbsent = true)

    // Step 2: Read adgroup dataset
    // day 1's adgroup data is exported at the end of day 0
    val adGroupData = AdGroupDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)

    // Step 3: Calculate real optout rate by joining to bids, along with other bid-level metrics
    val pcResultsGeronimoData = PcResultsMergedDataset.readDate(date, env = envForRead, nullIfColAbsent = true)

    val totalBidData = unionBidAndOptOutData(
      adGroupData = adGroupData,
      pcOptOutData=pcOptOutData,
      pcBidData=pcResultsGeronimoData
    )

    val bidOptOutMetrics = aggregateBidOptOutData(totalBidData)

    // Step 4: Combine and write metrics needed for monitoring purposes
    val backoffMetricsData = combineMetrics(date, backoffMetrics, bidOptOutMetrics, finalCampaignThrottleData)

    // Step 5 (Hold on this): Label Campaigns?


    BackoffMonitoringDataset.writeData(date, backoffMetricsData, fileCount)

  }
}
