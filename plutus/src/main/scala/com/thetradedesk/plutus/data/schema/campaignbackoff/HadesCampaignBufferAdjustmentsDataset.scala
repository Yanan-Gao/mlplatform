package com.thetradedesk.plutus.data.schema.campaignbackoff

import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.platformWideBuffer
import com.thetradedesk.plutus.data.utils.S3DailyParquetDataset
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.time.LocalDate
import scala.reflect.ClassTag

object HadesCampaignBufferAdjustmentsDataset extends S3DailyParquetDataset[HadesBufferAdjustmentSchema] {
  val DATA_VERSION = 3

  override protected def genBasePath(env: String): String = {
    f"s3://thetradedesk-mlplatform-us-east-1/env=${env}/data/plutusbackoff/hadesbufferadjustments/v=${DATA_VERSION}"
  }

  def readLatestDataUpToIncluding
  (maxDate: LocalDate, env: String, nullIfColAbsent: Boolean, historyLength: Int)
  (implicit encoder: Encoder[HadesBufferAdjustmentSchema], spark: SparkSession): Dataset[HadesBufferAdjustmentSchema] = {
    this.readLatestDataUpToIncluding(maxDate, env = env, nullIfColAbsent = nullIfColAbsent)
      .map(row =>
        row.copy(
          // Subtracting 1 from the context window length so we can add one back
          CampaignType_Previous = getNewHistory(row.CampaignType_Previous, row.CampaignType, historyLength),
          Hades_isProblemCampaign_Previous = getNewHistory(row.Hades_isProblemCampaign_Previous, row.Hades_isProblemCampaign, historyLength),
          AdjustmentQuantile_Previous = getNewHistory(row.AdjustmentQuantile_Previous, row.AdjustmentQuantile, historyLength),
          HadesBackoff_FloorBuffer_Previous = getNewHistory(row.HadesBackoff_FloorBuffer_Previous, row.HadesBackoff_FloorBuffer_Current, historyLength),
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
          BBF_FloorBuffer = row.BBF_FloorBuffer.orElse(Some(platformWideBuffer)),
          Actual_BBF_FloorBuffer = row.Actual_BBF_FloorBuffer.orElse(Some(platformWideBuffer)),
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

case class HadesBufferAdjustmentSchema(
                                    CampaignId: String,

                                    CampaignType: String,
                                    CampaignType_Previous: Array[String] = Array(),

                                    // This is the aggregate adjustment
                                    HadesBackoff_FloorBuffer: Double,

                                    // This is the adjustment just based on recent data
                                    HadesBackoff_FloorBuffer_Current: Double,
                                    HadesBackoff_FloorBuffer_Previous: Array[Double] = Array(),

                                    // These are the different quantiles within which we choose one option
                                    // Based on the AdjustmentQuantile
                                    HadesBackoff_FloorBuffer_Options: Array[Double] = Array(),

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

                                    BBF_FloorBuffer: Option[Double],
                                    Actual_BBF_FloorBuffer: Option[Double]
                                  )

case class HadesV3CampaignStats(
                               CampaignId: String,
                               CampaignType: String,

                               Actual_BBF_FloorBuffer: Double,

                               HadesBackoff_FloorBuffer_Options: Array[Double] = Array(),

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