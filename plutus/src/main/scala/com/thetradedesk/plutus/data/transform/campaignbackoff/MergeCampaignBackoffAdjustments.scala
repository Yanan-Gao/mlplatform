package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsDataset, CampaignAdjustmentsPacingSchema, CampaignAdjustmentsSchema, CampaignFloorBufferDataset, CampaignFloorBufferSchema, HadesAdjustmentSchemaV2, MergedCampaignAdjustmentsDataset}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform.platformWideBuffer
import com.thetradedesk.spark.TTDSparkContext.spark
import job.campaignbackoff.CampaignAdjustmentsJob.{date, fileCount, numRowsWritten}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object MergeCampaignBackoffAdjustments {

  def addPrefix(df: DataFrame, prefix: String, exceptColumn: String = "CampaignId"): org.apache.spark.sql.DataFrame = {
    df.columns.foldLeft(df) { (tempDf, colName) =>
      if (colName != exceptColumn) tempDf.withColumnRenamed(colName, s"$prefix$colName") else tempDf
    }
  }

  def mergeBackoffDatasets(campaignAdjustmentsPacingDataset: Dataset[CampaignAdjustmentsPacingSchema],
                           hadesCampaignAdjustmentsDataset: Dataset[HadesAdjustmentSchemaV2],
                           todaysCampaignFloorBufferSnapshot: Dataset[CampaignFloorBufferSchema]
                          )
  : DataFrame = {
    addPrefix(campaignAdjustmentsPacingDataset.toDF(), "pc_")
      .join(broadcast(addPrefix(hadesCampaignAdjustmentsDataset.toDF(), "hd_")), Seq("CampaignId"), "fullouter")
      .join(broadcast(addPrefix(todaysCampaignFloorBufferSnapshot.toDF(), "bf_")), Seq("CampaignId"), "fullouter")
      .withColumn("MergedPCAdjustment", least(lit(1.0), col("pc_CampaignPCAdjustment"), col("hd_HadesBackoff_PCAdjustment")))
      .withColumn("CampaignBbfFloorBuffer", coalesce(col("hd_BBF_FloorBuffer"), col("bf_BBF_FloorBuffer"), lit(HadesCampaignAdjustmentsTransform.platformWideBuffer)))
  }

  def transform(plutusCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsPacingSchema], hadesCampaignAdjustmentsDataset: Dataset[HadesAdjustmentSchemaV2]): Unit = {
    // Merging Campaign Backoff, Hades Backoff and any remaining campaigns with non platform wide floor buffer
    val todaysCampaignFloorBufferSnapshot = CampaignFloorBufferDataset.readLatestDataUpToIncluding(date.plusDays(1), envForReadInternal)
      .distinct()
    val finalMergedAdjustments = mergeBackoffDatasets(
      plutusCampaignAdjustmentsDataset,
      hadesCampaignAdjustmentsDataset,
      todaysCampaignFloorBufferSnapshot
    )
      .persist(StorageLevel.MEMORY_ONLY_2)

    // Writing the full dataset used in future jobs
    MergedCampaignAdjustmentsDataset.writeDataframe(date, finalMergedAdjustments, fileCount)

    val finalCampaignCount = finalMergedAdjustments.count()
    numRowsWritten.set(finalCampaignCount)

    // Writing just the adjustments to s3; This is imported to provisioning
    // The provisioning import job expects the final adjustment to be called CampaignPCAdjustment
    val campaignAdjustmentsWithFloorBuffer = finalMergedAdjustments
      .select("CampaignId", "MergedPCAdjustment", "CampaignBbfFloorBuffer")
      .withColumnRenamed("MergedPCAdjustment", "CampaignPCAdjustment")
      .filter((col("CampaignPCAdjustment").isNotNull && col("CampaignPCAdjustment") < 1)
        || (col("CampaignBbfFloorBuffer").isNotNull && col("CampaignBbfFloorBuffer") =!= platformWideBuffer))
      .selectAs[CampaignAdjustmentsSchema]

    CampaignAdjustmentsDataset.writeData(date, campaignAdjustmentsWithFloorBuffer, fileCount)
  }
}
