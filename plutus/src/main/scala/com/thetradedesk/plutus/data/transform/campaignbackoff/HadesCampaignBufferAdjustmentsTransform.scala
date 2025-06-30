package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.{CampaignFloorBufferSchema, MergedCampaignFloorBufferDataset, MergedCampaignFloorBufferSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{Campaign, bucketCount, getTestBucketUDF, platformWideBuffer}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData, PlutusOptoutBidsDataset}
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import com.thetradedesk.plutus.data.{AuctionType, envForRead, envForReadInternal}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord, CampaignDataSet}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignAdjustmentsJob.{hadesCampaignCounts}
import org.apache.hadoop.shaded.org.apache.commons.math3.special.Erf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

object HadesCampaignBufferAdjustmentsTransform {

  // Constants
  val floor = 0.0
  val goldenRatio = (math.sqrt(5.0) + 1.0) / 2.0
  val valCloseTo0 = 1e-10

  val HistoryLength = 10
  val DefaultAdjustmentQuantile = 50

  val EPSILON = 0.01
  val MinimumFloorBuffer = 0.01
  val MaximumFloorBuffer = 0.85

  val MinTestBucketIncluded = 0.0
  val MaxTestBucketExcluded = 0.9

  def cdf(mu: Double, sigma: Double, x: Double): Double = {
    //val a = (math.log(x) - mu) / math.sqrt(2 * math.pow(sigma, 2))
    val a = (math.log(x) - mu) / (math.sqrt(2) * sigma)
    val b = 0.5 + 0.5 * Erf.erf(a) // not from import org.apache.commons.math3.special._
    return b
  }

  def gssFunc(mu: Double, sigma: Double, bid: Double, epsilon: Double): Double = {
    var bMin = floor
    var bMax = bid

    var x1 = bMax - (bMax - bMin) / goldenRatio
    var x2 = bMin + (bMax - bMin) / goldenRatio

    for (i <- 1 to 100) {
      val surplus1 = (bid - x1) * cdf(mu, sigma, x1)
      val surplus2 = (bid - x2) * cdf(mu, sigma, x2)
      if (surplus1 > surplus2) {
        bMax = x2
      } else {
        bMin = x1
      }
      if ((bMax - bMin) < epsilon) {
        return (bMax + bMin) / 2
      }
      x1 = bMax - (bMax - bMin) / goldenRatio
      x2 = bMin + (bMax - bMin) / goldenRatio
    }
    return (bMin + bMax) / 2
  }

  def getUnderdeliveringCampaignBidData(bidData: DataFrame,
    filteredCampaigns: Dataset[CampaignMetaData]): DataFrame = {

    val gss = udf((m: Double, s: Double, b: Double, e: Double) => gssFunc(m, s, b, e))

    bidData
      // Exclude rare cases with initial bids below the floor to avoid skewing the median (this includes BBF Gauntlet bids)
      .filter(col("FloorPrice") < col("InitialBid"))
      .join(broadcast(filteredCampaigns), Seq("CampaignId"), "inner")
      .withColumn("MaxBidCpmInBucks", coalesce(col("MaxBidCpmInBucks"), col("InitialBid"))) // Null check
      .withColumn("Market", // Define Market only using AuctionType and if has DealId
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      // We want to potentially calculate floor buffers for bids where
      // PC pushdown is applied directly (model='plutus') or where
      // we calculate pc pushdown but dont apply pushdown (fixed price bids)
      .filter(col("Model") === "plutus" || col("Market") === "Fixed")
      // Update Initial Bid value used to calculate gss for the Propeller bids
      .withColumn("gen_initialBid",
        when(col("UseUncappedBidForPushdown"),
          when(col("MaxBidMultiplierCap").isNotNull,
            least(col("UncappedBidPrice"), col("MaxBidCpmInBucks") * greatest(col("MaxBidMultiplierCap"), lit(1.0)))
          ).otherwise(col("UncappedBidPrice"))
        ).otherwise(col("InitialBid")))
      .withColumn("gen_discrepancy", when(col("AuctionType") === AuctionType.FixedPrice, lit(1)).otherwise(when(col("Discrepancy") === 0, lit(1)).otherwise(col("Discrepancy"))))
      // old gen_gss_pushdown logic: .withColumn("gen_gss_pushdown", when(col("AuctionType") =!= AuctionType.FixedPrice, $"GSS").otherwise(gss(col("Mu"), col("Sigma"), col("InitialBid"), lit(0.1)) / col("InitialBid")))
      // updated for propeller gen_gss_pushdown logic: Use gen_initialBid when calculating gen_gss_pushdown for Propeller. Can't use GSS in pcgeronimo directly because apply effectiveMaxBid cap
      // Using InitialBid here instead of gen_initialBid because in Bidder, GSS is calculated as (tensorflowPcModelBid / InitialBid)
      .withColumn("gen_tensorflowPcModelBid", when(col("AuctionType") === AuctionType.FixedPrice, gss(col("Mu"), col("Sigma"), col("gen_initialBid"), lit(EPSILON))).otherwise(col("Gss") * col("InitialBid")))
      .withColumn("gen_gss_pushdown",
        // for Uncapped Bids, InitialBid is effectively MaxBid (not gen_initialBid)
        when(col("UseUncappedBidForPushdown"), least(col("gen_tensorflowPcModelBid"), col("InitialBid")) / col("InitialBid"))
          .otherwise(col("gen_tensorflowPcModelBid") / col("InitialBid")))
      .withColumn("gen_effectiveDiscrepancy", least(lit(1), lit(1) / col("gen_discrepancy")))
      .withColumn("gen_excess", col("gen_effectiveDiscrepancy") - col("gen_gss_pushdown"))

      // Exclude cases where we just apply the minimum pushdown (discrepancy)
      .filter(col("gen_excess") > 0)
      .withColumn("gen_actual_bufferFloor", col("FloorPrice") * (lit(1) - col("Actual_BBF_FloorBuffer")))
      .withColumn("gen_proposedBid_v3", col("InitialBid") * (
          col("gen_effectiveDiscrepancy") - (col("gen_excess") * coalesce(col("CampaignPCAdjustment"), lit(1.0)))
        ))
      .withColumn("gen_isBidBelowTheFloor", col("gen_proposedBid_v3") < col("gen_actual_bufferFloor"))
      .withColumn("gen_HadesBackoff_FloorBuffer",
        when(
          col("gen_isBidBelowTheFloor"),
          lit(1) - (col("gen_proposedBid_v3") / col("FloorPrice"))
        ).otherwise(col("Actual_BBF_FloorBuffer"))
      )
      // How many PC bids would've fallen below the actual buffer floor?
      // In other words, how many PC bids would've been opted out in our current bidding environment?
      .withColumn("BBF_PMP_Bid",
        when(col("Market").isin("Variable", "Fixed") && col("gen_isBidBelowTheFloor"), true)
          .otherwise(false))
      .withColumn("BBF_OM_Bid",
        when(col("Market").isin("OpenMarket") && col("gen_isBidBelowTheFloor"), true)
          .otherwise(false))
  }

  def aggregateCampaignBBFOptOutRate(campaignBidData: DataFrame,
                                     campaignThrottleData: Dataset[CampaignThrottleMetricSchema]): Dataset[HadesV3CampaignStats] = {
    val campaignUnderdeliveryData = campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction")
      )

    campaignBidData
      .groupBy("CampaignId", "CampaignType", "Actual_BBF_FloorBuffer")
      .agg(
        count("*").as("Total_BidCount"),
        sum(
          when( col("Market").isin("Variable", "Fixed"), lit(1))
            .otherwise(lit(0))
        ).as("Total_PMP_BidCount"),
        sum(
          when( col("Market") === "Fixed", col("FloorPrice"))
            .when( col("Market") === "Variable", col("FinalBidPrice"))
            .otherwise(lit(0))
        ).as("Total_PMP_BidAmount"),
        sum(
          when(col("BBF_PMP_Bid"),lit(1)).otherwise(lit(0))
        ).as("BBF_PMP_BidCount"),
        sum(
          when(col("BBF_PMP_Bid"), when(col("Market") === "Fixed", col("FloorPrice"))
            .otherwise(col("FinalBidPrice"))
          ).otherwise(lit(0))
        ).as("BBF_PMP_BidAmount"),
        sum(
          when( col("Market") === "OpenMarket", lit(1)).otherwise(lit(0))
        ).as("Total_OM_BidCount"),
        sum(
          when( col("Market") === "OpenMarket", col("FinalBidPrice")).otherwise(lit(0))
        ).as("Total_OM_BidAmount"),
        sum(
          when(col("BBF_OM_Bid"),col("FinalBidPrice")).otherwise(lit(0))
        ).as("BBF_OM_BidAmount"),
        sum(
          when(col("BBF_OM_Bid"),lit(1)).otherwise(lit(0))
        ).as("BBF_OM_BidCount"),
        // This is a workaround we cannot use AdjustmentQuantile here
        // because spark doesn't support column values in this function
        // Also, AdjustmentQuantile is calculated later.
        //expr("percentile_approx(gen_HadesBackoff_FloorBuffer, array(0.5, 0.4, 0.3), 200)").as("HadesBackoff_FloorBuffer_Options")
        expr("percentile_approx(gen_HadesBackoff_FloorBuffer, array(0.5, 0.4, 0.3), 200)").as("HadesBackoff_FloorBuffer_Options")
      )
      .join(broadcast(campaignUnderdeliveryData), Seq("CampaignId"), "left")
      .as[HadesV3CampaignStats]
  }

  case class PlutusCampaignAdjustment(CampaignId: String, CampaignPCAdjustment: Double)
  case class CampaignMetaData(CampaignId: String, CampaignType: String, Actual_BBF_FloorBuffer: Double, CampaignPCAdjustment: Double)
  case class HadesMetrics(CampaignType: String, PacingType: String, OptoutType: String, AdjustmentQuantile: Int, Count: Long, Buffer: String)

  val CampaignType_NewCampaign = "CampaignWithNewAdjustment";
  val CampaignType_AdjustedCampaign = "CampaignWithOldAdjustment";
  val CampaignType_NoAdjustment = "CampaignWithNoAdjustment";

  val PacingStatus_NoPacingData = "NoPacingData"
  val PacingStatus_NotPacing = "NotPacing"
  val PacingStatus_Pacing = "Pacing"

  val OptoutStatus_HighOptout = "HighOptOut"
  val OptoutStatus_LowOptout = "LowOptOut"
  val OptoutStatus_NoBids = "NoBids"

  def getFilteredCampaigns(campaignThrottleData: Dataset[CampaignThrottleMetricSchema],
                           campaignFloorBuffer: Dataset[MergedCampaignFloorBufferSchema],
                           campaignAdjustmentsPacing: Dataset[PlutusCampaignAdjustment],
                           potentiallyNewCampaigns: Dataset[Campaign],
                           adjustedCampaigns: Dataset[Campaign],
                           underdeliveryThreshold: Double): Dataset[CampaignMetaData] = {

    val campaignUnderdeliveryData = campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        first($"IsValuePacing").as("IsValuePacing"),
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction")
      )

    // Campaigns with no prior adjustments and no underdelivery data
    val newOrNonSpendingCampaigns = potentiallyNewCampaigns
      .join(campaignUnderdeliveryData, Seq("CampaignId"), "left_anti")
      // Filter for Buffer Test Campaigns only
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
      .join(adjustedCampaigns, Seq("CampaignId"), "left_anti")
      .select("CampaignId")
      .withColumn("CampaignType", lit(CampaignType_NewCampaign))

    // Campaigns with no prior adjustments and with underdelivery data
    val newUnderDeliveringCampaigns = campaignUnderdeliveryData
      .join(adjustedCampaigns, Seq("CampaignId"), "left_anti")
      .filter(col("UnderdeliveryFraction") >= underdeliveryThreshold)
      // Filter for DA Buffer Test Campaigns only
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter( // Filter for Buffer Test DA Campaigns only
        col("IsValuePacing") &&
          (col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
      )
      .select("CampaignId")
      .withColumn("CampaignType", lit(CampaignType_NewCampaign))

    val yesterdaysCampaigns = adjustedCampaigns
      .withColumn("CampaignType", lit(CampaignType_AdjustedCampaign))
      .select("CampaignId", "CampaignType")

    val candidateSelection_campaignFloorBuffer = campaignFloorBuffer.withColumnRenamed("BBF_FloorBuffer", "CandidateSelection_BBF_FloorBuffer")

    val res = newOrNonSpendingCampaigns
      .union(newUnderDeliveringCampaigns)
      .union(yesterdaysCampaigns)
      .join(campaignAdjustmentsPacing, Seq("CampaignId"), "left") // Join to get Plutus Backoff value to use for Strategy in calculation
      .join(candidateSelection_campaignFloorBuffer, Seq("CampaignId"), "left") // Join to get actual Floor buffer
      .withColumn("Actual_BBF_FloorBuffer", // Log each campaign's actual floor buffer if no buffer backoff to use as minimum cap for buffer
        when(col("CandidateSelection_BBF_FloorBuffer").isNotNull, $"CandidateSelection_BBF_FloorBuffer")
          .otherwise(lit(platformWideBuffer)))
      .selectAs[CampaignMetaData]
      .cache()

    res
  }

  def identifyAndHandleProblemCampaigns(
                                         todaysData: Dataset[HadesV3CampaignStats],
                                         yesterdaysData: Dataset[HadesBufferAdjustmentSchema],
                                         underdeliveryThreshold: Double
                                       ): (Dataset[HadesBufferAdjustmentSchema], Array[HadesMetrics]) = {
    val res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData, underdeliveryThreshold).persist(StorageLevel.MEMORY_ONLY_2)

    val metrics = res
      .withColumn("PacingType",
        when($"UnderdeliveryFraction".isNull, PacingStatus_NoPacingData)
          .when($"UnderdeliveryFraction" > underdeliveryThreshold, PacingStatus_NotPacing)
          .otherwise(PacingStatus_Pacing))
      .withColumn("OptoutType",
        when(col("Total_BidCount") === lit(0), OptoutStatus_NoBids)
          .when(col("BBF_PMP_BidAmount") / (col("Total_PMP_BidAmount") + col("Total_OM_BidAmount")) > lit(0.5), OptoutStatus_HighOptout)
          .otherwise(OptoutStatus_LowOptout))
      .withColumn("Buffer", format_number(round(col("BBF_FloorBuffer") / 0.05, 0) * 0.05, 2)) // Quantized
      .groupBy("CampaignType", "PacingType", "OptoutType", "AdjustmentQuantile", "Buffer")
      .count().as[HadesMetrics]
      .collect()

    (res, metrics)
  }

  def getFinalAdjustment(currentAdjustment: Double, previousAdjustments: Array[Double], underdeliveryFraction: Option[Double], underdeliveryThreshold: Double = 0.05, contextSize: Int = 2): Double = {
    if (previousAdjustments == null || previousAdjustments.length == 0) {
      return Math.max(0.01, currentAdjustment)
    }

    val adjustmentsToConsider = previousAdjustments.takeRight(contextSize - 1) // Including the current adjustment
    val rollingAverageAdjustment = (adjustmentsToConsider.sum + currentAdjustment) / (adjustmentsToConsider.length + 1)
    if (underdeliveryFraction.isEmpty || underdeliveryFraction.get < underdeliveryThreshold) {
      // If campaign is delivering, we don't need to make big changes
      Math.max(0.01, rollingAverageAdjustment)
    } else {
      // If campaign is not delivering, we will take the most aggressive pushdown
      Array(0.01, currentAdjustment, rollingAverageAdjustment).max
    }
  }

  def getCurrentAdjustment(campaignId: String, adjustmentQuantile: Int, adjustmentOptions: Array[Double]): Double = {
    adjustmentQuantile match {
      case 50 => adjustmentOptions(0)
      case 45 => (adjustmentOptions(0) + adjustmentOptions(1)) / 2
      case 40 => adjustmentOptions(1)
      case 35 => (adjustmentOptions(1) + adjustmentOptions(2)) / 2
      case 30 => adjustmentOptions(2)
      case _ => throw new IllegalArgumentException(s"Unexpected adjustmentQuantile: $adjustmentQuantile")
    }
  }

  val quantileAdjustmentMinimum = 30

  val quantileAdjustmentSlopeThreshold = -0.02

  /**
   * AdjustmentQuantile should be decreased if the following conditions are met:
   * - No previous Quantile Adjustment within context window
   * - The campaign always needed adjustment within the context window (had high underdelivery & high optout rate)
   * - Underdelivery is almost the same or increasing within the context window
   * - Optout Rate is almost the same or increasing within the context window
   *
   * If the above conditions are met, we reduce the adjustment quantile by 5 points
   *
   */
  def getAdjustmentQuantile(previousQuantiles: Array[Int],
                            underdeliveryFraction_Current: Option[Double], underdeliveryFraction_Previous: Array[Option[Double]],
                            total_BidCount: Double, total_BidCount_Previous: Array[Double],
                            bbf_pmp_BidCount: Double, bbf_pmp_BidCount_Previous: Array[Double],
                            underdeliveryThreshold: Double,
                            contextSize: Int = 4): Int = {

    if (previousQuantiles == null || previousQuantiles.length == 0)
      return DefaultAdjustmentQuantile

    val yesterdaysQuantile = previousQuantiles.last
    val quantilesInContext = previousQuantiles.takeRight(contextSize)

    // ensure that no adjustment has been made to the quantile for the past x days
    val isQuantileEligibleForChange = quantilesInContext.forall(_ == yesterdaysQuantile) && previousQuantiles.length >= contextSize && yesterdaysQuantile != quantileAdjustmentMinimum

    val bidcount_all = total_BidCount_Previous.takeRight(contextSize-1) :+ total_BidCount
    val bidcount_bbf_pmp = bbf_pmp_BidCount_Previous.takeRight(contextSize-1) :+ bbf_pmp_BidCount
    val shareOfBids_bbf_pmp = bidcount_bbf_pmp.zip(bidcount_all).map { case (a, b) => a / b }

    val underdelivery = underdeliveryFraction_Previous.takeRight(contextSize-1) :+ underdeliveryFraction_Current

    // ensure that campaign has been not pacing due to Optout for the past x days
    val doesCampaignNeedAdjustment = shareOfBids_bbf_pmp.forall( _ > (yesterdaysQuantile / 100)) &&
      underdelivery.forall(u => {
        u.nonEmpty && u.get > underdeliveryThreshold
      })

    if (!isQuantileEligibleForChange || !doesCampaignNeedAdjustment) {
      yesterdaysQuantile
    } else {
      val isBBFShareOfBidsReducingOverTime = isReducingOverTime(shareOfBids_bbf_pmp, quantileAdjustmentSlopeThreshold)
      val isUnderdeliveryReducingOverTime = isReducingOverTime(underdelivery.map(_.get), quantileAdjustmentSlopeThreshold)

      if (!isBBFShareOfBidsReducingOverTime && !isUnderdeliveryReducingOverTime) {
        yesterdaysQuantile - 5
      } else {
        yesterdaysQuantile
      }
    }
  }

  /**
   * Slope of  1 is /
   * Slope of  0 is -
   * Slope of -1 is \
   * To check if an array is reducing over time,
   * we want the slope to be < threshold
   *
   * @param arr
   * @param threshold If threshold is slightly less than 0, we will only
   *                  return false if arr is holding steady
   * @return
   */
  def isReducingOverTime(arr: Array[Double], threshold: Double): Boolean = {
    val n = arr.length
    if (n < 2) throw new IllegalArgumentException("Array must have at least two elements")

    val x = arr.indices.map(_.toDouble)  // x values (indices)
    val y = arr                          // y values (array elements)

    val sumX = x.sum
    val sumY = y.sum
    val sumXY = (x zip y).map { case (xi, yi) => xi * yi }.sum
    val sumX2 = x.map(xi => xi * xi).sum

    val numerator = sumXY - (sumX * sumY) / n
    val denominator = sumX2 - (sumX * sumX) / n

    val slope = numerator / denominator

    println(f"For values: ${arr.mkString("Array(", ", ", ")")}, Slope: $slope, Threshold: $threshold")
    println(f"slope > threshold: ${slope > threshold}")

    slope < threshold
  }

  def countTrailingZeros(arr: Array[Long]): Int = {
    // Added to handle when Total_PMP_BidCount_Previous & Total_OM_BidCount_Previous are null
    // Not sure why this didn't throw an error before?
    if (arr == null) return 0

    var count = 0
    var i = arr.length - 1

    while (i >= 0 && arr(i) == 0) {
      count += 1
      i -= 1
    }
    count
  }

  private def getFinalAdjustmentUDF: UserDefinedFunction = udf(getFinalAdjustment _)
  private def getCurrentAdjustmentUDF: UserDefinedFunction = udf(getCurrentAdjustment _)
  private def getAdjustmentQuantileUDF: UserDefinedFunction = udf(getAdjustmentQuantile _)
  private def countTrailingZerosUDF: UserDefinedFunction = udf(countTrailingZeros _)

  def mergeTodayWithYesterdaysData(todaysData: Dataset[HadesV3CampaignStats], yesterdaysData: Dataset[HadesBufferAdjustmentSchema], underdeliveryThreshold: Double) : Dataset[HadesBufferAdjustmentSchema] = {
    val selectedCols = yesterdaysData.columns.filter(_.endsWith("_Previous")) :+ "AdjustmentQuantile"
    val yesterdaysAdjustments = yesterdaysData.select("CampaignId", selectedCols: _*)

    val currentAdjustments = todaysData
      .join(broadcast(yesterdaysAdjustments), Seq("CampaignId") , "left_outer")
      .withColumn("AdjustmentQuantile",
        coalesce(getAdjustmentQuantileUDF(
          col("AdjustmentQuantile_Previous"),
          col("UnderdeliveryFraction"), col("UnderdeliveryFraction_Previous"),
          col("Total_BidCount"), col("Total_BidCount_Previous"),
          col("BBF_PMP_BidCount"), col("BBF_PMP_BidCount_Previous"),
          lit(underdeliveryThreshold),
          lit(4), // Context size of x: Adjustment Quantile will be adjusted only if not adjusted for *x* days
        ), lit(DefaultAdjustmentQuantile)))
      .withColumn("HadesBackoff_FloorBuffer_Current",
        // We want to apply adjustment when PMP OptOut Fraction is high. We dont want to apply an adjustment when
        // just OM BBF Fraction is high. This check makes sure that OM BBF Fraction is low.
        when((col("BBF_OM_BidCount") === lit(0)) || col("BBF_OM_BidCount") / col("Total_BidCount") <= ((lit(100) - $"AdjustmentQuantile") / lit(100)),
          getCurrentAdjustmentUDF(
            col("CampaignId"),
            col("AdjustmentQuantile"),
            col("HadesBackoff_FloorBuffer_Options"))
        ).otherwise(col("Actual_BBF_FloorBuffer"))
      ).withColumn("HadesBackoff_FloorBuffer",
        when(col("CampaignType") === CampaignType_AdjustedCampaign ||
          col("BBF_PMP_BidCount") / col("Total_BidCount") > ($"AdjustmentQuantile" / lit(100)),
          getFinalAdjustmentUDF(
            col("HadesBackoff_FloorBuffer_Current"),
            col("HadesBackoff_FloorBuffer_Previous"),
            col("UnderdeliveryFraction"),
            lit(underdeliveryThreshold),
            lit(4) // Context Size of x: FinalAdjustment = min(average of last*x* adjustments including current, current adjustment)
          )
        ).otherwise(col("Actual_BBF_FloorBuffer"))
      )
      .withColumn("Hades_isProblemCampaign",
        coalesce(
          (col("BBF_PMP_BidAmount") / (col("Total_PMP_BidAmount") + col("Total_OM_BidAmount")) > ($"AdjustmentQuantile" / lit(100))) && (col("UnderdeliveryFraction") > underdeliveryThreshold),
          lit(false)
        )
      )
      .withColumn("BBF_FloorBuffer",
        // Max cap at 0.85
        when(col("HadesBackoff_FloorBuffer") >= MaximumFloorBuffer, lit(MaximumFloorBuffer))
          // Min cap at actual BBF Floor Buffer if no buffer test (0.01 or 0.35)
          .when(col("HadesBackoff_FloorBuffer") <= col("Actual_BBF_FloorBuffer"), $"Actual_BBF_FloorBuffer")
          // Handle in case of nulls
          .otherwise(coalesce($"HadesBackoff_FloorBuffer", $"Actual_BBF_FloorBuffer"))
      )
      .withColumn("CampaignType", when($"BBF_FloorBuffer" > $"Actual_BBF_FloorBuffer", $"CampaignType")
        .otherwise(lit(CampaignType_NoAdjustment)))
      .selectAs[HadesBufferAdjustmentSchema]

    val oldAdjustments = yesterdaysData
      .join(broadcast(todaysData), Seq("CampaignId") , "left_anti")
      .filter(countTrailingZerosUDF(col("Total_PMP_BidCount_Previous")) < 5 ||
        countTrailingZerosUDF(col("Total_OM_BidCount_Previous")) < 5) // Drop adjustment if more than x if days of 0 bids in historical data

      .withColumn("Hades_isProblemCampaign", lit(false))
      .withColumn("HadesBackoff_FloorBuffer_Current", col("HadesBackoff_FloorBuffer"))
      .withColumn("CampaignType",
        // Min cap at actual BBF Floor Buffer if no buffer test (0.01 or 0.35)
        when(col("HadesBackoff_FloorBuffer") > col("Actual_BBF_FloorBuffer"), lit(CampaignType_AdjustedCampaign))
          .otherwise(lit(CampaignType_NoAdjustment))
      )
      .withColumn("HadesBackoff_FloorBuffer_Options", array())
      .selectAs[HadesBufferAdjustmentSchema]

    currentAdjustments union oldAdjustments
  }

  def getAllBidData(pcOptoutData: Dataset[PlutusLogsData], adGroupData: Dataset[AdGroupRecord], pcResultsMergedData: Dataset[PcResultsMergedSchema]): DataFrame = {
    val adGroupDistinctData = adGroupData
      // Filter for Buffer Test Campaigns only
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
      // We exclude campaigns with PC Enabled = false
      .filter(col("PredictiveClearingEnabled"))
      .select("AdGroupId", "CampaignId").distinct()

    val plutusLogsData = pcOptoutData
      .filter($"BidBelowFloorExceptedSource" === 2)
      .join(adGroupDistinctData, Seq("AdGroupId"), "inner")
      .drop("LegacyPcPushdown", "LogEntryTime")
      .toDF()

    val columns = plutusLogsData.columns

    val validAdgroups = adGroupDistinctData.select("AdGroupId").distinct()

    pcResultsMergedData
      .drop("LogEntryTime")
      .join(validAdgroups, Seq("AdGroupId"), "inner")
      .toDF
      .select(columns.head, columns.drop(1): _*)
      .union(plutusLogsData)
  }

  def transform(date: LocalDate,
                underdeliveryThreshold: Double,
                fileCount: Int,
                campaignFloorBufferData: Dataset[MergedCampaignFloorBufferSchema],
                campaignAdjustmentsPacingData: Dataset[CampaignAdjustmentsPacingSchema]
               ): Dataset[HadesBufferAdjustmentSchema] = {

    val yesterdaysData = (try {
      HadesCampaignBufferAdjustmentsDataset.readLatestDataUpToIncluding(
        date.minusDays(1),
        env = envForReadInternal,
        nullIfColAbsent = true,
        historyLength = HistoryLength
      )
    } catch {
      case _: S3NoFilesFoundException =>
        Seq.empty[HadesBufferAdjustmentSchema].toDS()
    })
    // Filter for Buffer Test Campaigns only
    .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
    .filter(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
    .as[HadesBufferAdjustmentSchema].cache()

    val campaignUnderdeliveryData = CampaignThrottleMetricDataset.readDate(env = envForRead, date = date)
      .cache()

    val pcResultsMergedData = PcResultsMergedDataset.readDate(env = envForRead, date = date, nullIfColAbsent = true)

    val pcOptoutData = PlutusOptoutBidsDataset.readDate(env = envForRead, date = date, nullIfColAbsent = true)

    // day 1's campaign data is exported at the end of day 0
    val nonArchivedCampaigns = CampaignDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)
      .filter($"IsVisible")
      .select("CampaignId").distinct()

    val liveCampaigns = CampaignFlightDataset.readLatestDataUpToIncluding(date.plusDays(1))
      .filter($"IsCurrent" === 1 && $"StartDateInclusiveUTC" <= date && $"EndDateExclusiveUTC" >= date)
      .join(nonArchivedCampaigns, Seq("CampaignId"), "inner")
      .selectAs[Campaign].distinct()

    val filteredCampaigns = getFilteredCampaigns(
      campaignThrottleData = campaignUnderdeliveryData,
      campaignFloorBuffer = campaignFloorBufferData,
      campaignAdjustmentsPacing = campaignAdjustmentsPacingData
        .filter(col("IsTest"))
        .select("CampaignId", "CampaignPCAdjustment").distinct().as[PlutusCampaignAdjustment],
      potentiallyNewCampaigns = liveCampaigns,
      adjustedCampaigns = yesterdaysData
        .filter($"HadesBackoff_FloorBuffer".isNotNull && $"HadesBackoff_FloorBuffer" > lit(MinimumFloorBuffer))
        // Filter for Buffer Test Campaigns only
        .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
        .filter(col("TestBucket") >= (lit(bucketCount) * MinTestBucketIncluded) && col("TestBucket") < (lit(bucketCount) * MaxTestBucketExcluded))
        .select("CampaignId").as[Campaign],
      underdeliveryThreshold
    )

    // day 1's adgroup data is exported at the end of day 0
    val adGroupData = AdGroupDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)
      .join(filteredCampaigns, Seq("CampaignId"), "inner")
      .selectAs[AdGroupRecord]

    // Combine bid data from both pcOptout dataset and pcResultsMerged Dataset
    val bidData = getAllBidData(pcOptoutData, adGroupData, pcResultsMergedData)

    // Get bid data filtered to underdelivering campaigns
    val campaignBidData = getUnderdeliveringCampaignBidData(bidData, filteredCampaigns)
    // Get Optout Rates & potential pushdowns for underdelivering campaigns
    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData, campaignUnderdeliveryData).cache()

    // Get final pushdowns
    val (hadesBufferAdjustmentsDataset, metrics) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData, underdeliveryThreshold)

    HadesCampaignBufferAdjustmentsDataset.writeData(date, hadesBufferAdjustmentsDataset, fileCount)

    val hadesIsProblemCampaignsCount = hadesBufferAdjustmentsDataset.filter(col("Hades_isProblemCampaign") === true).count()
    val hadesTotalAdjustmentsCount = hadesBufferAdjustmentsDataset.filter(col("HadesBackoff_FloorBuffer") < 1.0).count()

    import job.campaignbackoff.CampaignAdjustmentsJob.hadesBackoffV3Metrics

    hadesCampaignCounts.labels(Map("status" -> "HadesProblemCampaigns")).set(hadesIsProblemCampaignsCount)
    hadesCampaignCounts.labels(Map("status" -> "HadesAdjustedCampaigns")).set(hadesTotalAdjustmentsCount)
    metrics.foreach { metric =>
      hadesBackoffV3Metrics.labels(Map(
        "CampaignType" -> metric.CampaignType,
        "Pacing" -> metric.PacingType,
        "OptOut" -> metric.OptoutType,
        "Quantile" -> metric.AdjustmentQuantile.toString,
        "Backoff" -> metric.Buffer)
      ).set(metric.Count)
    }

    hadesBufferAdjustmentsDataset
  }
}

