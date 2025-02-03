package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.envForWrite
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsDataset, MergedCampaignAdjustmentsDataset, MergedCampaignAdjustmentsSchema, CampaignAdjustmentsSchema, CampaignAdjustmentsHadesSchema, CampaignAdjustmentsPacingSchema}
import com.thetradedesk.plutus.data.transform.campaignbackoff.PlutusCampaignAdjustmentsTransform.mergeBackoffDatasets
import job.campaignbackoff.CampaignAdjustmentsJob.{date, fileCount, numRowsWritten}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

object MergeCampaignBackoffAdjustments {

  def transform(plutusCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsPacingSchema], hadesCampaignAdjustmentsDataset: Dataset[CampaignAdjustmentsHadesSchema]): Unit = {
    // Merging Campaign Backoff with Hades Backoff
    val finalMergedAdjustments = mergeBackoffDatasets(plutusCampaignAdjustmentsDataset, hadesCampaignAdjustmentsDataset)
      .persist(StorageLevel.MEMORY_ONLY_2)

    // Writing the full dataset used in future jobs
    MergedCampaignAdjustmentsDataset.writeData(date, finalMergedAdjustments, fileCount)

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
