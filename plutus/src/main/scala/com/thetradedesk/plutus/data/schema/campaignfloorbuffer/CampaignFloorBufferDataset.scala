package com.thetradedesk.plutus.data.schema.campaignfloorbuffer

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset

import java.time.LocalDate

object CampaignFloorBufferDataset extends S3DailyParquetDataset[CampaignFloorBufferSchema]{
  val DATA_VERSION = 1
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignfloorbuffer/v=${DATA_VERSION}"
  }
}

object CampaignFloorBufferRollbackDataset extends S3DailyParquetDataset[CampaignFloorBufferSchema]{
  val DATA_VERSION = 1
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignfloorbufferrollback/v=${DATA_VERSION}"
  }
}

object MergedCampaignFloorBufferDataset extends S3DailyParquetDataset[MergedCampaignFloorBufferSchema]{
  val DATA_VERSION = 1
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/mergedfloorbuffer/v=${DATA_VERSION}"
  }
}

case class CampaignFloorBufferSchema(
                                            CampaignId: String,
                                            BBF_FloorBuffer: Double,
                                            // Date the campaign and floor buffer was added to the dataset on meeting the selection criteria.
                                            AddedDate: LocalDate
                                          )

case class MergedCampaignFloorBufferSchema(
                                            CampaignId: String,
                                            BBF_FloorBuffer: Double,
                                            // Date here is either the date the campaign met the selection criteria or was found as the experiment candidate.
                                            AddedDate: LocalDate,
                                            BufferType: String
                                    )
