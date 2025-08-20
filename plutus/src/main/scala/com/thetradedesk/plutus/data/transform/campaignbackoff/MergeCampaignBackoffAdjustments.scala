package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.platformWideBuffer
import com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff.VirtualMaxBidBackoffSchema
import com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff.VirtualMaxBidBackoffTransform.VirtualMaxBid_Multiplier_Platform
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignAdjustmentsJob.{date, fileCount, numRowsWritten, shortFlightCampaignCounts}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

object MergeCampaignBackoffAdjustments {

  val MinimumShortFlightFloorBuffer = 0.35

  def addPrefix(df: DataFrame, prefix: String, exceptColumn: String = "CampaignId"): org.apache.spark.sql.DataFrame = {
    df.columns.foldLeft(df) { (tempDf, colName) =>
      if (colName != exceptColumn) tempDf.withColumnRenamed(colName, s"$prefix$colName") else tempDf
    }
  }

  def countTrailingZeros(arr: Array[Long]): Int = {
    if (arr == null) return 0
    var count = 0
    var i = arr.length - 1
    while (i >= 0 && arr(i) == 0.0) {
      count += 1
      i -= 1
    }
    count
  }

  def getShortFlightBuffer(
                                BBF_FloorBuffer: Double,
                                MinimumShortFlightFloorBuffer: Double,
                                HadesBackoff_FloorBuffer_Previous: Array[Double],
                                Total_BidCount_Previous: Array[Long]
                              ): Double = {
    // Short flight buffer logic:
    // - Applies only when MinimumShortFlightFloorBuffer is defined.
    //
    // Case 1: No Hades buffer history so hdv3_HadesBackoff_FloorBuffer_Previous is null or had 0 bids for past 4 days  (new short-flight campaign).
    // Case 2: 1–3 days of buffer history and has bid in previous few days (existing short-flight campaign without long-term consistent data).
    //   In both cases, the campaign is already being backed off today, so:
    //   → Ensure today's buffer is at least MinimumShortFlightFloorBuffer to be more aggressive in its short flight.
    //   → If today’s buffer = platformWideBuffer, it's likely fine so don't change.

    val hasPreviousBuffer = HadesBackoff_FloorBuffer_Previous != null && HadesBackoff_FloorBuffer_Previous.nonEmpty
    val hasPreviousBids = countTrailingZeros(Total_BidCount_Previous) < 4

    // Case 1: New short flight campaigns
    // These campaigns will have bids > 0 today, but not before today.
    val isNewShortFlight =
      (!hasPreviousBuffer || (hasPreviousBuffer && !hasPreviousBids)) &&
        BBF_FloorBuffer > platformWideBuffer &&
        BBF_FloorBuffer < MinimumShortFlightFloorBuffer

    val limitedBufferHistory = hasPreviousBuffer && HadesBackoff_FloorBuffer_Previous.size < 4

    // Case 2: Existing short flight campaigns with limited recent history (< 4 days)
    val isExistingShortFlightWithLimitedBufferHistory =
      limitedBufferHistory &&
        BBF_FloorBuffer > platformWideBuffer &&
        BBF_FloorBuffer < MinimumShortFlightFloorBuffer

    if (isNewShortFlight || isExistingShortFlightWithLimitedBufferHistory) {
      MinimumShortFlightFloorBuffer
    } else {
      BBF_FloorBuffer
    }
  }

  private def getShortFlightBufferUDF: UserDefinedFunction = udf(getShortFlightBuffer _)

  def mergeBackoffDatasets(campaignAdjustmentsPacingDataset: Dataset[CampaignAdjustmentsPacingSchema],
                           hadesCampaignBufferAdjustmentsDataset: Dataset[HadesBufferAdjustmentSchema],
                           shortFlightCampaignsDataset: Dataset[ShortFlightCampaignsSchema],
                           propellerBackoffDataset: Dataset[VirtualMaxBidBackoffSchema]
                          )
  : DataFrame = {
    addPrefix(campaignAdjustmentsPacingDataset.toDF(), "pc_")
      .join(broadcast(addPrefix(hadesCampaignBufferAdjustmentsDataset.toDF(), "hdv3_")), Seq("CampaignId"), "fullouter")
      .join(broadcast(addPrefix(shortFlightCampaignsDataset.toDF(), "sf_")), Seq("CampaignId"), "fullouter")
      .join(broadcast(addPrefix(propellerBackoffDataset.toDF(), "pb_")), Seq("CampaignId"), "fullouter")
      .withColumn("MergedPCAdjustment", least(lit(1.0), col("pc_CampaignPCAdjustment")))
      .withColumn("VirtualMaxBid_Multiplier", coalesce(col("pb_VirtualMaxBid_Multiplier"), lit(VirtualMaxBid_Multiplier_Platform)))
      .withColumn("CampaignBbfFloorBuffer",
        // If hdv3_HadesBackoff_FloorBuffer is null (or it's a future short-flight campaign),
        // the final `otherwise` fallback will assign MinimumShortFlightFloorBuffer if available.
        when(col("sf_MinimumShortFlightFloorBuffer").isNotNull && col("hdv3_BBF_FloorBuffer").isNotNull,
          getShortFlightBufferUDF(
            col("hdv3_BBF_FloorBuffer"),
            col("sf_MinimumShortFlightFloorBuffer"),
            col("hdv3_HadesBackoff_FloorBuffer_Previous"),
            col("hdv3_Total_BidCount_Previous"))
        ).otherwise(
          coalesce(col("hdv3_BBF_FloorBuffer"), col("sf_MinimumShortFlightFloorBuffer"), lit(platformWideBuffer))
        )
      )
  }

  def transform(plutusCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsPacingSchema],
                hadesCampaignBufferAdjustmentsDataset: Dataset[HadesBufferAdjustmentSchema],
                shortFlightCampaignsDataset: Dataset[ShortFlightCampaignsSchema],
                propellerBackoffDataset: Dataset[VirtualMaxBidBackoffSchema]
               ): Unit = {
    // Merging Campaign Backoff, Hades Backoff, Adhoc test campaigns and any remaining campaigns with non platform wide floor buffer

    val finalMergedAdjustments = mergeBackoffDatasets(
      plutusCampaignAdjustmentsDataset,
      hadesCampaignBufferAdjustmentsDataset,
      shortFlightCampaignsDataset,
      propellerBackoffDataset
    )
      .persist(StorageLevel.MEMORY_ONLY_2)

    // Writing the full dataset used in future jobs
    MergedCampaignAdjustmentsDataset.writeDataframe(date, finalMergedAdjustments, fileCount)

    val finalCampaignCount = finalMergedAdjustments.count()
    numRowsWritten.set(finalCampaignCount)

    val shortFlightAdjustedCampaignsCount = finalMergedAdjustments
      .filter(col("sf_MinimumShortFlightFloorBuffer") === MinimumShortFlightFloorBuffer && col("CampaignBbfFloorBuffer") === MinimumShortFlightFloorBuffer)
      .count()
    shortFlightCampaignCounts.labels(Map("status" -> "ShortFlightAdjustedCampaigns")).set(shortFlightAdjustedCampaignsCount)

    // Writing just the adjustments to s3; This is imported to provisioning
    // The provisioning import job expects the final adjustment to be called CampaignPCAdjustment
    val campaignAdjustmentsWithFloorBuffer = getCampaignAdjustments(finalMergedAdjustments)

    CampaignAdjustmentsDataset.writeData(date, campaignAdjustmentsWithFloorBuffer, fileCount)
  }

  def getCampaignAdjustments(finalMergedAdjustments: DataFrame): Dataset[CampaignAdjustmentsSchema] = {
    finalMergedAdjustments
      .withColumnRenamed("MergedPCAdjustment", "CampaignPCAdjustment")
      .withColumnRenamed("VirtualMaxBid_Multiplier", "CampaignVirtualMaxBidMultiplierCap")
      .filter((col("CampaignPCAdjustment").isNotNull && col("CampaignPCAdjustment") < 1)
        || (col("CampaignBbfFloorBuffer").isNotNull && col("CampaignBbfFloorBuffer") =!= platformWideBuffer)
        || (col("CampaignVirtualMaxBidMultiplierCap").isNotNull && col("CampaignVirtualMaxBidMultiplierCap") > lit(VirtualMaxBid_Multiplier_Platform)))
      .selectAs[CampaignAdjustmentsSchema]
  }
}
