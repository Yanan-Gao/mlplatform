package com.thetradedesk.plutus.data.transform.virtualmaxbidbackoff

import com.thetradedesk.plutus.data.{envForRead, envForReadInternal}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PcResultsMergedSchema, PlutusLogsData, PlutusOptoutBidsDataset}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignThrottleMetricDataset, CampaignThrottleMetricSchema}
import com.thetradedesk.plutus.data.schema.campaignbackoff.HadesCampaignAdjustmentsDataset.getNewHistory
import com.thetradedesk.plutus.data.schema.virtualmaxbidbackoff.{VirtualMaxBidBackoffDataset, VirtualMaxBidBackoffSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{bucketCount, getTestBucketUDF}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform.{PacingStatus_NoPacingData, PacingStatus_NotPacing, PacingStatus_Pacing}
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import org.apache.spark.sql.Dataset
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignAdjustmentsJob.virtualMaxBidBackoffMetrics
import org.apache.spark.sql.functions._

import java.time.LocalDate

object VirtualMaxBidBackoffTransform {

  // Configuration
  val CampaignBucketStart = 0;
  val CampaignBucketEnd = 100;
  val UnderdeliveryThreshold = 0.075;

  val VirtualMaxBidCap_Quantile_Adjustment = 10
  val VirtualMaxBidCap_Quantile_Default = 90;

  val VirtualMaxBid_Multiplier_Ceiling = 4
  val VirtualMaxBid_Multiplier_Platform = 2

  val BidCloseToMaxBid_Buffer = 0.9

  val WaitingPeriod = 3 //days
  val HistoryLength = 10 //days

  case class RelevantBidData(
    CampaignId: String,
    UncappedBidPrice: Double,
    MaxBidCpmInBucks: Double,
    InitialBid: Double,
    Channel: String
  )

  case class VirtualMaxBidCampaignData(
    CampaignId: String,
    UnderdeliveryFraction: Double,
    UnderdeliveryFraction_History: Array[Double],
    VirtualMaxBid_Quantile_History: Array[Int],
    BidsCloseToMaxBid_Fraction_History: Array[Double],
    SumInternalBidOverMaxBid_Fraction_History: Array[Double],
  )

  case class VirtualMaxBidMetrics(
    CampaignType: String,
    PacingType: String,
    VirtualMaxBid_Multiplier: String,
    VirtualMaxBid_Quantile: Int,
    Count: Long
  )

  def getAdjustmentFromQuantile(adjustmentQuantile: Int, adjustmentOptions: Seq[Double]): Double = {
    adjustmentQuantile match {
      case 90 => adjustmentOptions(4)
      case 80 => (adjustmentOptions(4) + adjustmentOptions(3)) / 2
      case 70 => adjustmentOptions(3)
      case 60 => (adjustmentOptions(3) + adjustmentOptions(2)) / 2
      case 50 => adjustmentOptions(2)
      case 40 => (adjustmentOptions(2) + adjustmentOptions(1)) / 2
      case 30 => adjustmentOptions(1)
      case 20 => (adjustmentOptions(1) + adjustmentOptions(0)) / 2
      case 10 => adjustmentOptions(0)
      case _ => throw new IllegalArgumentException(s"Unexpected adjustmentQuantile: $adjustmentQuantile")
    }
  }

  def getVirtualMaxBidQuantile(
    underdeliveryFraction: Double,
    underdeliveryFraction_History: Array[Double],
    virtualMaxBidQuantileHistory: Array[Int]
  ): Int = {
    if (virtualMaxBidQuantileHistory == null || virtualMaxBidQuantileHistory.length < WaitingPeriod) {
      // Case 1: If we dont have enough history, we return the default value
      return VirtualMaxBidCap_Quantile_Default
    }

    val lastQuantile = virtualMaxBidQuantileHistory.last
    val underdeliveryFractions_actual = underdeliveryFraction_History :+ underdeliveryFraction

    val (daysSinceLastChange, indexOfLastChange) = getLastChanges(virtualMaxBidQuantileHistory)

    if (indexOfLastChange > 0 && daysSinceLastChange <= WaitingPeriod) {
      // If there's been a recent change, check if underdelivery is down
      val daysUnderdelivered = underdeliveryFractions_actual.takeRight(daysSinceLastChange).count(_ >= UnderdeliveryThreshold)

      if (daysUnderdelivered > 0) {
        // Case 2: If there was a recent change to the quantile, and underdelivery went up,
        // we revert to the quantile before the change
        return virtualMaxBidQuantileHistory.apply(indexOfLastChange)
      }

      // Case 3: If there was a recent change to the quantile, and delivery is stable,
      // we dont change the quantile
      return lastQuantile
    }

    // Check if delivery and quantiles are stable
    val isDeliveryStable = underdeliveryFractions_actual.takeRight(WaitingPeriod).forall(_ < UnderdeliveryThreshold)

    if (isDeliveryStable) {
      // Case 4: If we have waited for `WaitingPeriod` days and delivery has been stable,
      // we lower the quantile

      // Note: we allow this to go below 10. If we see that any campaign is going below 10,
      // we will remove the adjustment from that campaign later right after this function call
      val newQuantile = virtualMaxBidQuantileHistory.last - VirtualMaxBidCap_Quantile_Adjustment
      return newQuantile
    }

    // Case 5: If we have waited for `WaitingPeriod` days and delivery has not been stable,
    // we dont change the quantile
    lastQuantile
  }

  // This is a separate function for ease of testing
  def getLastChanges(virtualMaxBidQuantileHistory: Array[Int]): (Int, Int) = {
    val lastQuantile = virtualMaxBidQuantileHistory.last

    // Check for recent virtualMaxBid quantile change
    val indexOfLastChange = virtualMaxBidQuantileHistory.lastIndexWhere(_ > lastQuantile)

    val daysSinceLastChange = if (indexOfLastChange >= 0) {
      virtualMaxBidQuantileHistory.length - indexOfLastChange - 1
    } else {
      virtualMaxBidQuantileHistory.length
    }
    // Todo: returning a tuple is a bad idea
    (daysSinceLastChange, indexOfLastChange)
  }

  val getAdjustmentFromQuantileUDF = udf(getAdjustmentFromQuantile _)
  val getVirtualMaxBidQuantileUDF = udf(getVirtualMaxBidQuantile _)

  def transform(
    date: LocalDate,
  ): Dataset[VirtualMaxBidBackoffSchema] = {
    val yesterdaysData = try {
      VirtualMaxBidBackoffDataset.readLatestDataUpToIncluding(
          date.minusDays(1), env = envForReadInternal, nullIfColAbsent = true)
        .map(row =>
          row.copy(
            // Subtracting 1 from the context window length so we can add one back
            UnderdeliveryFraction_History = getNewHistory(row.UnderdeliveryFraction_History, row.UnderdeliveryFraction, HistoryLength),
            VirtualMaxBid_Quantile_History = getNewHistory(row.VirtualMaxBid_Quantile_History, row.VirtualMaxBid_Quantile, HistoryLength),
            SumInternalBidOverMaxBid_Fraction_History = getNewHistory(row.SumInternalBidOverMaxBid_Fraction_History, row.SumInternalBidOverMaxBid_Fraction, HistoryLength),
          )
        )
        .cache()
    } catch {
      case _: S3NoFilesFoundException =>
        Seq.empty[VirtualMaxBidBackoffSchema]
          .toDS()
    }

    val campaignThrottleDataset = CampaignThrottleMetricDataset.readDate(env = envForRead, date = date)
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") >= CampaignBucketStart
        && col("TestBucket") < CampaignBucketEnd)
      .selectAs[CampaignThrottleMetricSchema]

    val relevantCampaigns = getRelevantCampaigns(yesterdaysData, campaignThrottleDataset)
      .cache()

    val relevantAdgroups = AdGroupDataSet().readLatestPartitionUpTo(date.plusDays(1), isInclusive = true)
      .join(relevantCampaigns, Seq("CampaignId"), "inner")
      .filter(col("PredictiveClearingEnabled"))
      .selectAs[AdGroupRecord]
    val optoutDataset = PlutusOptoutBidsDataset.readDate(env = envForRead, date = date, nullIfColAbsent = true)
    val pcbidDataset = PcResultsMergedDataset.readDate(env = envForRead, date = date, nullIfColAbsent = true)

    val relevantBidData = getRelevantBidData(relevantAdgroups, optoutDataset, pcbidDataset)

    val res = getVirtualMaxBidBackoffValues(relevantBidData, relevantCampaigns)
      .cache()

    VirtualMaxBidBackoffDataset.writeData(date, res, filecount = 10)

    getMetrics(res).foreach { metric =>
      virtualMaxBidBackoffMetrics.labels(Map(
        "CampaignType" -> metric.CampaignType,
        "Pacing" -> metric.PacingType,
        "Multiplier" -> metric.VirtualMaxBid_Multiplier,
        "Quantile" -> metric.VirtualMaxBid_Quantile.toString,
      )).set(metric.Count)
    }
    res
  }

  def getMetrics(res: Dataset[VirtualMaxBidBackoffSchema]): Array[VirtualMaxBidMetrics] = {
    res
      .withColumn("CampaignType",
        when($"VirtualMaxBid_Multiplier" === lit(VirtualMaxBid_Multiplier_Platform), "Not Adjusted")
        .when($"UnderdeliveryFraction_History".isNull, "New Campaign")
        .otherwise("Old Campaign"))
      .withColumn("PacingType",
        when($"UnderdeliveryFraction".isNull, PacingStatus_NoPacingData)
          .when($"UnderdeliveryFraction" > UnderdeliveryThreshold, PacingStatus_NotPacing)
          .otherwise(PacingStatus_Pacing))
      .withColumn("VirtualMaxBid_Multiplier", format_number(round(col("VirtualMaxBid_Multiplier") / 0.2, 0) * 0.2, 2)) // Quantized
      .groupBy("CampaignType", "PacingType", "VirtualMaxBid_Multiplier", "VirtualMaxBid_Quantile")
      .count()
      .as[VirtualMaxBidMetrics]
      .collect()
  }


  def getVirtualMaxBidBackoffValues(relevantBidData: Dataset[RelevantBidData], relevantCampaigns: Dataset[VirtualMaxBidCampaignData]): Dataset[VirtualMaxBidBackoffSchema] = {
    val aggCampaignData = relevantBidData
      // Keeping this filter till we get ImpressionMultiplier here so we can
      // adjust Internal bid using that for DOOH
      .filter($"Channel" =!= lit("Digital Out Of Home"))
      .withColumn("CappedInitialBid", least($"InitialBid", $"MaxBidCPMInBucks"))
      .withColumn("IsBidCloseToMaxBid", when(col("InitialBid") > (lit(BidCloseToMaxBid_Buffer) * col("MaxBidCpmInBucks")), lit(1)).otherwise(lit(0)))
      .withColumn("UncappedBidOverMaxBid", $"UncappedBidPrice" / $"MaxBidCpmInBucks")
      .groupBy("CampaignId")
      .agg(
        (sum("IsBidCloseToMaxBid") / count("*")).as("BidsCloseToMaxBid_Fraction"),
        (sum("CappedInitialBid") / sum("MaxBidCPMInBucks")).as("SumInternalBidOverMaxBid_Fraction"),
        expr("percentile_approx(UncappedBidOverMaxBid, array(0.1, 0.3, 0.5, 0.7, 0.9), 100)").as("VirtualMaxBid_Multiplier_Options"),
      )
      .cache()

    val relevantCampaignsWithAggData = relevantCampaigns
      .join(aggCampaignData, Seq("CampaignId"), "inner")
      .withColumn("VirtualMaxBid_Quantile", getVirtualMaxBidQuantileUDF(
        col("UnderdeliveryFraction"),
        col("UnderdeliveryFraction_History"),
        col("VirtualMaxBid_Quantile_History")
      ))
      // We filter out campaigns which don't need backoff
      .filter($"VirtualMaxBid_Quantile" >= 10)

    // def getAdjustmentFromQuantileUDF = udf(getAdjustmentFromQuantile _)
    relevantCampaignsWithAggData
      .withColumn("VirtualMaxBid_Multiplier_Uncapped",
        getAdjustmentFromQuantileUDF(col("VirtualMaxBid_Quantile"), col("VirtualMaxBid_Multiplier_Options")))
      .withColumn("VirtualMaxBid_Multiplier",
        greatest(
          lit(VirtualMaxBid_Multiplier_Platform), least(
            $"VirtualMaxBid_Multiplier_Uncapped",
            lit(VirtualMaxBid_Multiplier_Ceiling)
          )
        ))
      .selectAs[VirtualMaxBidBackoffSchema]
  }

  def getRelevantBidData(relevantAdgroups: Dataset[AdGroupRecord], optoutDataset: Dataset[PlutusLogsData], pcbidDataset: Dataset[PcResultsMergedSchema]): Dataset[RelevantBidData] = {
    optoutDataset
      .join(broadcast(relevantAdgroups.select("AdgroupId", "CampaignId").distinct()), Seq("AdgroupId"), "inner")
      .selectAs[RelevantBidData]
      .union(pcbidDataset
        .join(broadcast(relevantAdgroups.select("AdgroupId").distinct()), Seq("AdgroupId"), "inner")
        .selectAs[RelevantBidData])
  }

  def getRelevantCampaigns(yesterdaysData: Dataset[VirtualMaxBidBackoffSchema], campaignThrottleDataset: Dataset[CampaignThrottleMetricSchema]): Dataset[VirtualMaxBidCampaignData] = {
    val underdeliveringCampaigns = campaignThrottleDataset
      .filter(col("IsValuePacing")
        && col("IsBaseBidOptimized") != lit("AllAnchored")
        && col("IsProgrammaticGuaranteed") != lit("PG"))
      .groupBy("CampaignId")
      .agg(
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction")
      )
      .cache()

    val newUnderdeliveringCampaigns = underdeliveringCampaigns
      .join(yesterdaysData, Seq("CampaignId"), "left_anti")
      .filter($"UnderdeliveryFraction" > lit(UnderdeliveryThreshold))
      .withColumn("UnderdeliveryFraction_History", lit(null).cast("ARRAY<DOUBLE>"))
      .withColumn("VirtualMaxBid_Quantile_History", lit(null).cast("ARRAY<INT>"))
      .withColumn("BidsCloseToMaxBid_Fraction_History", lit(null).cast("ARRAY<DOUBLE>"))
      .withColumn("SumInternalBidOverMaxBid_Fraction_History", lit(null).cast("ARRAY<DOUBLE>"))
      .selectAs[VirtualMaxBidCampaignData]

    val oldAdjustedCampaigns = yesterdaysData
      .drop("UnderdeliveryFraction",
        "VirtualMaxBidQuantile",
        "SumInternalBidOverMaxBid_Fraction",
        "BidsCloseToMaxBid_Fraction",
        "VirtualMaxBid_Multiplier",
        "VirtualMaxBid_Multiplier_Uncapped",
        "VirtualMaxBid_Multiplier_Options")
      .join(underdeliveringCampaigns.select("CampaignId", "UnderdeliveryFraction"),
        Seq("CampaignId"), "left")
      .selectAs[VirtualMaxBidCampaignData]

    oldAdjustedCampaigns.unionByName(newUnderdeliveringCampaigns)
  }
}
