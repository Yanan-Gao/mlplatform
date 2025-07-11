package com.thetradedesk.plutus.data.transform.monitoring

import com.thetradedesk.plutus.data.envForRead
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData, PlutusOptoutBidsDataset}
import com.thetradedesk.plutus.data.schema.campaignbackoff.MergedCampaignAdjustmentsDataset
import com.thetradedesk.plutus.data.schema.monitoring.{BackoffMonitoringDataset, BackoffMonitoringSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{bucketCount, getTestBucketUDF}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions._

import java.time.LocalDate

object BackoffMonitoringTransform {

  def getHadesBufferBackoffMetrics(hadesBackOffData : DataFrame) = {
    hadesBackOffData.withColumn("Sim_BBF_BidCount", $"hdv3_BBF_PMP_BidCount"+$"hdv3_BBF_OM_BidCount")
      .withColumn("Sim_BBF_OptOut_Rate", $"Sim_BBF_BidCount" / $"hdv3_Total_BidCount")
      .withColumn("Sim_BBF_PMP_OptOut_Rate", $"hdv3_BBF_PMP_BidCount"/$"hdv3_Total_BidCount")
      .withColumnRenamed("hdv3_UnderdeliveryFraction", "Sim_UnderdeliveryFraction")
      .withColumnRenamed("pc_CampaignPCAdjustment", "CampaignPCAdjustment")
      .select("CampaignId", "Sim_BBF_OptOut_Rate", "Sim_BBF_PMP_OptOut_Rate", "Sim_UnderdeliveryFraction", "CampaignBbfFloorBuffer", "CampaignPCAdjustment")
  }

  def getBbfOptOutRate(
                        date: LocalDate,
                        pcResultsGeronimoData: Dataset[PcResultsMergedSchema],
                        adGroupData: Dataset[AdGroupRecord],
                        pcOptOutData: Dataset[PlutusLogsData]
                      ): DataFrame = {

    val agg_pcOptoutData = pcOptOutData
      .join(broadcast(adGroupData), Seq("AdGroupId"), "left")
      .filter(col("IsValuePacing"))
      .groupBy("CampaignId")
      .agg(
        sum(when(col("BidBelowFloorExceptedSource") === 2, lit(1)).otherwise(0)).as("actual_BBF_OptOut_Bids")
      )
      .distinct()
      .cache()

    pcResultsGeronimoData
      .filter(col("IsValuePacing"))
      .groupBy("CampaignId")
      .agg(
        count("*").as("Bids")
      )
      .join(broadcast(agg_pcOptoutData), Seq("CampaignId"), "inner")
      .withColumn("actual_BBF_OptOut_Rate", col("actual_BBF_OptOut_Bids") / (col("Bids") + col("actual_BBF_OptOut_Bids")))
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount).cast("int")))
      .withColumn(
        "BucketGroup",
        when(col("TestBucket").between(0, 899), "Hades Test Group")
          .when(col("TestBucket").between(900, 1000), "Hades Control Group")
          .otherwise("Out of defined range")
      )
      .withColumn("date", lit(date))
  }

  def combineMetrics(
                      hadesBufferBackoffMetrics: DataFrame,
                      actualBbfOptoutMetrics: DataFrame
                    ) = {
    actualBbfOptoutMetrics
      .join(broadcast(hadesBufferBackoffMetrics), Seq("CampaignId"), "outer")
      .selectAs[BackoffMonitoringSchema]
  }

  def transform(date: LocalDate, fileCount: Int): Unit = {

    // Get metrics from the buffer backoff job data
    val hadesBufferBackoffData = MergedCampaignAdjustmentsDataset.readDataframe(date, env = envForRead)
    val hadesBufferBackoffMetrics = getHadesBufferBackoffMetrics(hadesBackOffData = hadesBufferBackoffData)

    // Calculate real optout metrics:
    // Step 1 : Read PC Optout data
    val pcOptOutData = PlutusOptoutBidsDataset.readDate(date, env = envForRead)

    // Step 2: Read adgroup dataset
    // day 1's adgroup data is exported at the end of day 0
    val adGroupData = AdGroupDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)

    // Step 3: Calculate real optout rate
    val pcResultsGeronimoData = PcResultsMergedDataset.readDate(date, env = envForRead)

    val finalBbfOptoutRate = getBbfOptOutRate(
      date=date,
      pcResultsGeronimoData=pcResultsGeronimoData,
      pcOptOutData=pcOptOutData,
      adGroupData = adGroupData
    )

    // Step 4: Combine and write metrics needed for monitoring purposes
    val backoffMetricsData = combineMetrics(hadesBufferBackoffMetrics, finalBbfOptoutRate)
    BackoffMonitoringDataset.writeData(date, backoffMetricsData, fileCount)

  }
}
