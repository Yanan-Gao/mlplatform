package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.time.LocalDate
import scala.reflect.ClassTag

object HadesCampaignAdjustmentsDataset extends S3DailyParquetDataset[HadesAdjustmentSchemaV2] {
  val DATA_VERSION = 2

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/hadesadjustments/v=${DATA_VERSION}"
  }

  def readLatestDataUpToIncluding
          (maxDate: LocalDate, env: String, nullIfColAbsent: Boolean, historyLength: Int)
          (implicit encoder: Encoder[HadesAdjustmentSchemaV2], spark: SparkSession): Dataset[HadesAdjustmentSchemaV2] = {
    this.readLatestDataUpToIncluding(maxDate, env = env, nullIfColAbsent = nullIfColAbsent)
      .map(row =>
        row.copy(
          // Subtracting 1 from the context window length so we can add one back
          CampaignType_Previous = getNewHistory(row.CampaignType_Previous, row.CampaignType, historyLength),
          Hades_isProblemCampaign_Previous = getNewHistory(row.Hades_isProblemCampaign_Previous, row.Hades_isProblemCampaign, historyLength),
          AdjustmentQuantile_Previous = getNewHistory(row.AdjustmentQuantile_Previous, row.AdjustmentQuantile, historyLength),
          HadesBackoff_PCAdjustment_Previous = getNewHistory(row.HadesBackoff_PCAdjustment_Previous, row.HadesBackoff_PCAdjustment_Current, historyLength),
          UnderdeliveryFraction_Previous = getNewHistory(row.UnderdeliveryFraction_Previous, row.UnderdeliveryFraction, historyLength),
          Total_BidCount_Previous = getNewHistory(row.Total_BidCount_Previous, row.Total_BidCount, historyLength),
          Total_PMP_BidCount_Previous = getNewHistory(row.Total_PMP_BidCount_Previous, row.Total_PMP_BidCount, historyLength),
          Total_PMP_BidAmount_Previous = getNewHistory(row.Total_PMP_BidAmount_Previous, row.Total_PMP_BidAmount, historyLength),
          BBF_PMP_BidCount_Previous = getNewHistory(row.BBF_PMP_BidCount_Previous, row.BBF_PMP_BidCount, historyLength),
          BBF_PMP_BidAmount_Previous = getNewHistory(row.BBF_PMP_BidAmount_Previous, row.BBF_PMP_BidAmount, historyLength),
          Total_OM_BidCount_Previous = getNewHistory(row.Total_OM_BidCount_Previous, row.Total_OM_BidCount, historyLength),
          Total_OM_BidAmount_Previous = getNewHistory(row.Total_OM_BidAmount_Previous, row.Total_OM_BidAmount, historyLength),
          BBF_OM_BidCount_Previous = getNewHistory(row.BBF_OM_BidCount_Previous, row.BBF_OM_BidCount, historyLength),
          BBF_OM_BidAmount_Previous = getNewHistory(row.BBF_OM_BidAmount_Previous, row.BBF_OM_BidAmount, historyLength),
          // Setting these to zero. They will be overwritten for campaigns which are still bidding
          // For campaigns which have stopped bidding, these 0s will be used instead
          UnderdeliveryFraction = None,
          Total_BidCount = 0,
          Total_PMP_BidCount = 0,
          Total_PMP_BidAmount = 0,
          BBF_PMP_BidCount = 0,
          BBF_PMP_BidAmount = 0,
          Total_OM_BidCount = 0,
          Total_OM_BidAmount = 0,
          BBF_OM_BidCount = 0,
          BBF_OM_BidAmount = 0,
        )
      )
  }

  def getNewHistory[T: ClassTag](olderValues: Array[T], previousValue: T, historyLength: Int): Array[T] = {
    Option(olderValues) match {
      case Some(values) => values.takeRight(math.max(0, historyLength - 1)) :+ previousValue
      case None         => Array(previousValue)
    }
  }
}

case class HadesAdjustmentSchemaV2(
  CampaignId: String,

  CampaignType: String,
  CampaignType_Previous: Array[String] = Array(),

  // This is the aggregate adjustment
  HadesBackoff_PCAdjustment: Double,

  // This is the adjustment just based on recent data
  HadesBackoff_PCAdjustment_Current: Double,
  HadesBackoff_PCAdjustment_Previous: Array[Double] = Array(),

  // These are the different quantiles within which we choose one option
  // Based on the AdjustmentQuantile
  HadesBackoff_PCAdjustment_Options: Array[Double] = Array(),

  // Is Problem Campaign = BBF Share of *BidAmount* > 0.5 AND UnderdeliveryFraction > threshold
  // This is a signal for analysis. We dont gate anything on this.
  Hades_isProblemCampaign: Boolean,
  Hades_isProblemCampaign_Previous: Array[Boolean] = Array(),

  // Determines what quantile should be used to get an adjustment for this campaign
  // Should be between 50 and 100
  AdjustmentQuantile: Int,
  AdjustmentQuantile_Previous: Array[Int] = Array(),

  UnderdeliveryFraction: Option[Double],
  UnderdeliveryFraction_Previous: Array[Option[Double]] = Array(),

  Total_BidCount: Long,
  Total_BidCount_Previous: Array[Long] = Array(),

  // For PMP Bids/Optout Bids
  Total_PMP_BidCount: Long,
  Total_PMP_BidCount_Previous: Array[Long] = Array(),

  Total_PMP_BidAmount: Double,
  Total_PMP_BidAmount_Previous: Array[Double] = Array(),

  BBF_PMP_BidCount: Double,
  BBF_PMP_BidCount_Previous: Array[Double] = Array(),

  BBF_PMP_BidAmount: Double,
  BBF_PMP_BidAmount_Previous: Array[Double] = Array(),

  // For OM Bids/Optout Bids
  Total_OM_BidCount: Long,
  Total_OM_BidCount_Previous: Array[Long] = Array(),

  Total_OM_BidAmount: Double,
  Total_OM_BidAmount_Previous: Array[Double] = Array(),

  BBF_OM_BidCount: Double,
  BBF_OM_BidCount_Previous: Array[Double] = Array(),

  BBF_OM_BidAmount: Double,
  BBF_OM_BidAmount_Previous: Array[Double] = Array(),

  // TODO: Add BBF_FloorBuffer: Double,
)

@Deprecated
object HadesCampaignAdjustmentsDatasetV1 extends S3DailyParquetDataset[CampaignAdjustmentsHadesSchema] {
  val DATA_VERSION = 1

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/hadesadjustments/v=${DATA_VERSION}"
  }
}

@Deprecated
case class CampaignAdjustmentsHadesSchema(
  CampaignId: String,
  CampaignType: String,
  HadesBackoff_PCAdjustment: Double,
  Hades_isProblemCampaign: Boolean,
  BBFPC_OptOut_ShareOfBids: Option[Double],
  BBFPC_OptOut_ShareOfBidAmount: Option[Double],
  HadesBackoff_PCAdjustment_Current: Option[Double],
  HadesBackoff_PCAdjustment_Old: Option[Double],
  CampaignType_Yesterday: Option[String],
)

case class HadesCampaignStats(
                               CampaignId: String,
                               CampaignType: String,

                               // TODO: Add BBF_FloorBuffer: Double,

                               HadesBackoff_PCAdjustment_Options: Array[Double] = Array(),

                               UnderdeliveryFraction: Option[Double],

                               Total_BidCount: Long,

                               Total_PMP_BidCount: Long,
                               Total_PMP_BidAmount: Double,
                               BBF_PMP_BidCount: Double,
                               BBF_PMP_BidAmount: Double,

                               Total_OM_BidCount: Long,
                               Total_OM_BidAmount: Double,
                               BBF_OM_BidCount: Double,
                               BBF_OM_BidAmount: Double
)
