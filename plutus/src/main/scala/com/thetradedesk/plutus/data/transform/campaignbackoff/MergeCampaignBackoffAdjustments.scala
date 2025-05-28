package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsDataset, CampaignAdjustmentsPacingSchema, CampaignAdjustmentsSchema, HadesAdjustmentSchemaV2, HadesBufferAdjustmentSchema, MergedCampaignAdjustmentsDataset}
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.MergedCampaignFloorBufferSchema
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{bucketCount, getTestBucketUDF, platformWideBuffer}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignBufferAdjustmentsTransform.{MaxTestBucketExcluded, MinTestBucketIncluded}
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
                           campaignFloorBufferDataset: Dataset[MergedCampaignFloorBufferSchema],
                           hadesCampaignBufferAdjustmentsDataset: Dataset[HadesBufferAdjustmentSchema]
                          )
  : DataFrame = {
    addPrefix(campaignAdjustmentsPacingDataset.toDF(), "pc_")
      .join(broadcast(addPrefix(hadesCampaignAdjustmentsDataset.toDF(), "hd_")), Seq("CampaignId"), "fullouter")
      .join(broadcast(addPrefix(campaignFloorBufferDataset.toDF(), "bf_")), Seq("CampaignId"), "fullouter")
      .join(broadcast(addPrefix(hadesCampaignBufferAdjustmentsDataset.toDF(), "hdv3_")), Seq("CampaignId"), "fullouter")
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .withColumn("IsTestCampaign", when(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded), true).otherwise(false))
      .withColumn("MergedPCAdjustment",
        when(col("IsTestCampaign"), least(lit(1.0), col("pc_CampaignPCAdjustment")))
          .otherwise(least(lit(1.0), col("pc_CampaignPCAdjustment"), col("hd_HadesBackoff_PCAdjustment")))
      )
      // Floor buffer assigned in the CampaignBbfFloorBufferCandidateSelectionJob will take precedence.
      // It should ideally be same as hd_BBF_FloorBuffer since HadesCampaignAdjustmentsTransform reads the same data to set floor buffer.
      // If none of them is found, CampaignBbfFloorBuffer should be set to platformWideBuffer
      .withColumn("CampaignBbfFloorBuffer",
        when(col("IsTestCampaign"), coalesce(col("hdv3_BBF_FloorBuffer"), col("bf_BBF_FloorBuffer"), lit(platformWideBuffer)))
          .otherwise(coalesce(col("bf_BBF_FloorBuffer"), lit(platformWideBuffer)))
      )
  }

  def transform(plutusCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsPacingSchema],
                hadesCampaignAdjustmentsDataset: Dataset[HadesAdjustmentSchemaV2],
                campaignFloorBufferData: Dataset[MergedCampaignFloorBufferSchema],
                hadesCampaignBufferAdjustmentsDataset: Dataset[HadesBufferAdjustmentSchema]): Unit = {
    // Merging Campaign Backoff, Hades Backoff, Adhoc test campaigns and any remaining campaigns with non platform wide floor buffer

    val finalMergedAdjustments = mergeBackoffDatasets(
      plutusCampaignAdjustmentsDataset,
      hadesCampaignAdjustmentsDataset,
      campaignFloorBufferData,
      hadesCampaignBufferAdjustmentsDataset
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
