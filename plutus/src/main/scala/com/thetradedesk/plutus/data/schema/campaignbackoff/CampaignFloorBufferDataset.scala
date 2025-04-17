package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.utils.{S3DailyParquetDataset, S3NoFilesFoundException}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import org.apache.spark.sql.{AnalysisException, Dataset, Encoder, SparkSession}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

import java.time.LocalDate

object CampaignFloorBufferDataset extends S3DailyParquetDataset[CampaignFloorBufferSchema]{
  val DATA_VERSION = 1
  /** Base S3 path, derived from the environment */
  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/campaignfloorbuffer/v=${DATA_VERSION}"
  }
}

case class CampaignFloorBufferSchema(
                                            CampaignId: String,
                                            BBF_FloorBuffer: Double
                                          )
