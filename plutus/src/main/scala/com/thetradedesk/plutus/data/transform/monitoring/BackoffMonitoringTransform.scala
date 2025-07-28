package com.thetradedesk.plutus.data.transform.monitoring

import com.thetradedesk.plutus.data.{AuctionType, envForRead}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData, PlutusOptoutBidsDataset}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignThrottleMetricDataset, CampaignThrottleMetricSchema, MergedCampaignAdjustmentsDataset}
import com.thetradedesk.plutus.data.schema.monitoring.{BackoffMonitoringDataset, BackoffMonitoringSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{bucketCount, getTestBucketUDF}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.time.LocalDate

object BackoffMonitoringTransform {

  def getHadesBufferBackoffMetrics(hadesBackOffData : DataFrame) = {
    hadesBackOffData
      .withColumn("Sim_BBF_BidCount", $"hdv3_BBF_PMP_BidCount" + $"hdv3_BBF_OM_BidCount")
      // hdv3_Total_BidCount is sum of BBF Opt Out Bids and Bids
      .withColumn("Sim_BBF_OptOut_Rate", $"Sim_BBF_BidCount" / $"hdv3_Total_BidCount")
      .withColumn("Sim_BBF_PMP_OptOut_Rate", $"hdv3_BBF_PMP_BidCount" / $"hdv3_Total_BidCount")
      .withColumn("Sim_BBF_OM_OptOut_Rate", $"hdv3_BBF_OM_BidCount" / $"hdv3_Total_BidCount")
      .withColumnRenamed("pc_CampaignPCAdjustment", "CampaignPCAdjustment")
      .select("CampaignId", "Sim_BBF_OptOut_Rate", "Sim_BBF_PMP_OptOut_Rate", "Sim_BBF_OM_OptOut_Rate", "CampaignBbfFloorBuffer", "CampaignPCAdjustment")
  }

  def getBbfOptOutRate(
                        pcResultsGeronimoData: Dataset[PcResultsMergedSchema],
                        adGroupData: Dataset[AdGroupRecord],
                        pcOptOutData: Dataset[PlutusLogsData]
                      ): DataFrame = {

    val agg_pcOptoutData = pcOptOutData
      .join(broadcast(adGroupData), Seq("AdGroupId"), "inner") // Inner join to handle null pointer error when no CampaignId match
      .withColumn("Market",
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      .groupBy("CampaignId")
      .agg(
        max(col("IsValuePacing")).as("IsValuePacing"),
        max(col("MaxBidMultiplierCap")).as("MaxBidMultiplierCap"),
        sum(
          when(col("BidBelowFloorExceptedSource") === 2, lit(1))
            .otherwise(0)
        ).as("BBF_OptOut_Bids"),
        sum(
          when(col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed"), lit(1))
            .otherwise(lit(0))
        ).as("BBF_PMP_OptOut_Bids"),
        sum(
          when(col("BidBelowFloorExceptedSource") === 2 && col("Market") === "OpenMarket", lit(1)).otherwise(lit(0))
        ).as("BBF_OM_OptOut_Bids"),
        sum(
          when(col("BidBelowFloorExceptedSource") =!= 2, lit(1))
            .otherwise(0)
        ).as("NonBBF_OptOut_Bids"),
      )
      .distinct()
      .cache()

    val agg_pcResultsGeronimoData = pcResultsGeronimoData
      .withColumn("Market",
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      .groupBy("CampaignId")
      .agg(
        max(col("IsValuePacing")).as("IsValuePacing"),
        max(col("MaxBidMultiplierCap")).as("MaxBidMultiplierCap"),
        count("*").as("Bids"),
        sum(
          when( col("Market").isin("Variable", "Fixed"), lit(1))
            .otherwise(lit(0))
        ).as("PMP_Bids"),
        sum(
          when( col("Market") === "OpenMarket", lit(1)).otherwise(lit(0))
        ).as("OM_Bids"),
      )

    agg_pcResultsGeronimoData.as("pcg")
      .join(broadcast(agg_pcOptoutData.as("pcoo")), Seq("CampaignId"), "outer")
      .select(
        col("CampaignId"),
        // Handle any mismatches in IsValuePacing so no duplicate campaign rows
        coalesce(col("pcg.IsValuePacing"), col("pcoo.IsValuePacing")).as("IsValuePacing"),
        // Handle any mismatches in MaxBidMultiplierCap so no duplicate campaign rows
        coalesce(col("pcg.MaxBidMultiplierCap"), col("pcoo.MaxBidMultiplierCap")).as("MaxBidMultiplierCap"),
        coalesce(col("pcg.Bids"), lit(0L)).as("Bids"),
        coalesce(col("pcg.PMP_Bids"), lit(0L)).as("PMP_Bids"),
        coalesce(col("pcg.OM_Bids"), lit(0L)).as("OM_Bids"),
        coalesce(col("pcoo.BBF_OptOut_Bids"), lit(0L)).as("BBF_OptOut_Bids"),
        coalesce(col("pcoo.BBF_PMP_OptOut_Bids"), lit(0L)).as("BBF_PMP_OptOut_Bids"),
        coalesce(col("pcoo.BBF_OM_OptOut_Bids"), lit(0L)).as("BBF_OM_OptOut_Bids"),
        coalesce(col("pcoo.NonBBF_OptOut_Bids"), lit(0L)).as("NonBBF_OptOut_Bids"),
      )
      .withColumn("BBF_OptOut_Rate", col("BBF_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      .withColumn("BBF_PMP_OptOut_Rate", col("BBF_PMP_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      .withColumn("BBF_OM_OptOut_Rate", col("BBF_OM_OptOut_Bids") / (col("Bids") + col("BBF_OptOut_Bids")))
      // Following OptOut Rate does not include BBF Opt Out
      .withColumn("NonBBF_OptOut_Rate", col("NonBBF_OptOut_Bids") / (col("Bids") + col("NonBBF_OptOut_Bids")))
  }

  def combineMetrics(
                      date: LocalDate,
                      hadesBufferBackoffMetrics: DataFrame,
                      actualBbfOptoutMetrics: DataFrame,
                      campaignThrottleData: Dataset[CampaignThrottleMetricSchema]
                    ) = {

    val campaignUnderdeliveryData = campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        first($"IsValuePacing").as("IsValuePacing"),
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction")
      )

    val joinBbfOptOutMetricsAndUnderdelivery = actualBbfOptoutMetrics.as("optOut")
      .join(broadcast(campaignUnderdeliveryData.as("udf")), Seq("CampaignId"), "left")
      .select(
        col("CampaignId"),
        col("optOut.MaxBidMultiplierCap"),
        col("optOut.BBF_OptOut_Rate"),
        col("optOut.BBF_PMP_OptOut_Rate"),
        col("optOut.BBF_OM_OptOut_Rate"),
        col("optOut.NonBBF_OptOut_Rate"),
        col("optOut.Bids"),
        col("optOut.PMP_Bids"),
        col("optOut.OM_Bids"),
        col("optOut.BBF_OptOut_Bids"),
        col("optOut.BBF_PMP_OptOut_Bids"),
        col("optOut.BBF_OM_OptOut_Bids"),
        col("optOut.NonBBF_OptOut_Bids"),
        // Handle any mismatches in IsValuePacing so no duplicate campaign rows
        coalesce(col("optOut.IsValuePacing"), col("udf.IsValuePacing")).as("IsValuePacing"),
        col("udf.UnderdeliveryFraction")
      )

    joinBbfOptOutMetricsAndUnderdelivery
      .join(broadcast(hadesBufferBackoffMetrics), Seq("CampaignId"), "left")
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

  def transform(date: LocalDate, fileCount: Int): Unit = {

    // Get metrics from the buffer backoff job data
    val hadesBufferBackoffData = MergedCampaignAdjustmentsDataset.readDataframe(date, env = envForRead)
    val hadesBufferBackoffMetrics = getHadesBufferBackoffMetrics(hadesBackOffData = hadesBufferBackoffData)

    // Get metrics from throttle dataset
    val campaignThrottleData = CampaignThrottleMetricDataset.readDate(env = envForRead, date = date)

    // Calculate real optout metrics:
    // Step 1 : Read PC Optout data
    val pcOptOutData = PlutusOptoutBidsDataset.readDate(date, env = envForRead)

    // Step 2: Read adgroup dataset
    // day 1's adgroup data is exported at the end of day 0
    val adGroupData = AdGroupDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)

    // Step 3: Calculate real optout rate
    val pcResultsGeronimoData = PcResultsMergedDataset.readDate(date, env = envForRead, nullIfColAbsent = true)

    val finalBbfOptoutRate = getBbfOptOutRate(
      pcResultsGeronimoData=pcResultsGeronimoData,
      pcOptOutData=pcOptOutData,
      adGroupData = adGroupData
    )

    // Step 4: Combine and write metrics needed for monitoring purposes
    val backoffMetricsData = combineMetrics(date, hadesBufferBackoffMetrics, finalBbfOptoutRate, campaignThrottleData)
    BackoffMonitoringDataset.writeData(date, backoffMetricsData, fileCount)

  }
}
