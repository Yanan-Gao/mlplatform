package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsDataset, HadesAdjustmentSchemaV2, CampaignAdjustmentsPacingSchema, CampaignAdjustmentsSchema, MergedCampaignAdjustmentsDataset}
import job.campaignbackoff.CampaignAdjustmentsJob.{date, fileCount, numRowsWritten}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
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
                           hadesCampaignAdjustmentsDataset: Dataset[HadesAdjustmentSchemaV2])
  : DataFrame = {
    addPrefix(campaignAdjustmentsPacingDataset.toDF(), "pc_")
      .join(broadcast(addPrefix(hadesCampaignAdjustmentsDataset.toDF(), "hd_")), Seq("CampaignId"), "fullouter")
      .withColumn("MergedPCAdjustment", least(lit(1.0), col("pc_CampaignPCAdjustment"), col("hd_HadesBackoff_PCAdjustment")))
  }

  def transform(plutusCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsPacingSchema], hadesCampaignAdjustmentsDataset: Dataset[HadesAdjustmentSchemaV2]): Unit = {
    // Merging Campaign Backoff with Hades Backoff
    val finalMergedAdjustments = mergeBackoffDatasets(plutusCampaignAdjustmentsDataset, hadesCampaignAdjustmentsDataset)
      .persist(StorageLevel.MEMORY_ONLY_2)

    // Writing the full dataset used in future jobs
    MergedCampaignAdjustmentsDataset.writeDataframe(date, finalMergedAdjustments, fileCount)

    // Writing just the adjustments to s3; This is imported to provisioning
    // The provisioning import job expects the final adjustment to be called CampaignPCAdjustment
    val onlyCampaignAdjustments = finalMergedAdjustments
      .select("CampaignId", "MergedPCAdjustment")
      .withColumnRenamed("MergedPCAdjustment", "CampaignPCAdjustment")
      .filter(col("CampaignPCAdjustment").isNotNull && col("CampaignPCAdjustment") < 1)
      .selectAs[CampaignAdjustmentsSchema]

    CampaignAdjustmentsDataset.writeData(date, onlyCampaignAdjustments, fileCount)

    val finalCampaignCount = finalMergedAdjustments.count()
    numRowsWritten.set(finalCampaignCount)
  }
}
