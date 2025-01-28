package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.schema.campaignbackoff.CampaignFlightDataset.loadParquetCampaignFlightLatestPartitionUpTo
import com.thetradedesk.plutus.data.{AuctionType, envForRead, loadParquetDataDailyV2}
import com.thetradedesk.plutus.data.schema.{PcResultsMergedDataset, PlutusLogsData, PlutusLogsDataset}
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsHadesSchema, CampaignFlightDataset, CampaignThrottleMetricDataset, CampaignThrottleMetricSchema}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.{AdGroupDataSet, AdGroupRecord, CampaignDataSet}
import org.apache.hadoop.shaded.org.apache.commons.math3.special.Erf
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate
import scala.util.hashing.MurmurHash3

object HadesCampaignAdjustmentsTransform {

  // Constants
  val bucketCount = 1000
  val buffer = 0.65

  val getTestBucketUDF = udf(computeBudgetBucketHash(_: String, _: Int))

  val floor = 0.0
  val goldenRatio = (math.sqrt(5.0) + 1.0) / 2.0
  val valCloseTo0 = 1e-10

  def computeBudgetBucketHash(entityId: String, bucketCount: Int): Short = {
    // Budget Bucket Hash Function
    // Tested here: https://dbc-7ae91121-86fd.cloud.databricks.com/editor/notebooks/1420435698064795?o=2560362456103657#command/1420435698066605
    Math.abs(MurmurHash3.stringHash(entityId) % bucketCount).toShort
  }

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
                                        filteredCampaigns: Dataset[FilteredCampaignData]): DataFrame = {

    val gss = udf((m: Double, s: Double, b: Double, e: Double) => gssFunc(m, s, b, e))

    bidData
      .filter(col("FloorPrice") < col("InitialBid")) // Exclude rare cases with initial bids below the floor to avoid skewing the median
      .join(broadcast(filteredCampaigns), Seq("CampaignId"), "inner")
      .withColumn("Market", // Define Market only using AuctionType and if has DealId
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      .withColumn("gen_discrepancy", when(col("AuctionType") === AuctionType.FixedPrice, lit(1)).otherwise(when(col("Discrepancy") === 0, lit(1)).otherwise(col("Discrepancy"))))
      .withColumn("gen_gss_pushdown", when(col("AuctionType") =!= AuctionType.FixedPrice, $"GSS").otherwise(gss(col("Mu"), col("Sigma"), col("InitialBid"), lit(0.1)) / col("InitialBid")))
      .withColumn("gen_effectiveDiscrepancy", least(lit(1), lit(1) / col("gen_discrepancy")))
      .withColumn("gen_excess", col("gen_effectiveDiscrepancy") - col("gen_gss_pushdown"))
      .filter(col("gen_excess") > 0)
      .withColumn("gen_bufferFloor", (col("FloorPrice") * lit(1 - buffer)))
      .withColumn("gen_plutusPushdownAtBufferFloor", col("gen_bufferFloor") / col("InitialBid"))

      // TODO: For CampaignType_AdjustedCampaignNotPacing campaigns, we could come up with a more aggressive
      //       Pushdown if we can verify that it helps.
      .withColumn("gen_PCAdjustment", (col("gen_effectiveDiscrepancy") - col("gen_plutusPushdownAtBufferFloor")) / (col("gen_effectiveDiscrepancy") - col("gen_gss_pushdown")))

      // We ignore the strategy here because then we calculate an adjustment which is independent
      // of previous adjustments, allowing the backoff to adapt to current bidding environment.
      .withColumn("gen_proposedBid", col("InitialBid") * col("gen_gss_pushdown"))
      .withColumn("gen_isInBufferZone", col("gen_proposedBid") >= col("gen_bufferFloor"))
      .withColumn("BBFPC_OptOutBid",
        when((col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed") && !col("gen_isInBufferZone")), true)
          .otherwise(false))
  }

  def aggregateCampaignBBFOptOutRate(campaignBidData: DataFrame): DataFrame = {
    campaignBidData
      .groupBy("CampaignId", "CampaignType")
      .agg(
        count("*").as("TotalBidCount_includingOptOut"),
        sum(col("FloorPrice")).as("TotalFloorPrice"),
        sum(col("FinalBidPrice")).as("TotalFinalBidPrice"),
        sum(
          when(
            col("BBFPC_OptOutBid") && col("Market") === "Variable",
            col("FinalBidPrice")
          ).otherwise(lit(0))
        ).as("BBFPC_OptOut_Variable_BidAmount"),
        sum(
          when(
            col("BBFPC_OptOutBid") && col("Market") === "Fixed",
            col("FloorPrice")
          ).otherwise(lit(0))
        ).as("BBFPC_OptOut_Fixed_BidAmount"),
        sum(
          when(
            col("BBFPC_OptOutBid") && col("Market") === "Variable",
            lit(1)
          ).otherwise(lit(0))
        ).as("BBFPC_OptOut_Variable_BidCount"),
        sum(
          when(
            col("BBFPC_OptOutBid") && col("Market") === "Fixed",
            lit(1)
          ).otherwise(lit(0))
        ).as("BBFPC_OptOut_Fixed_BidCount"),
        expr("percentile_approx(gen_PCAdjustment, 0.5, 1000)").as("HadesBackoff_PCAdjustment")
      )
      .withColumn("BBFPC_OptOut_Variable_ShareOfBidAmount",
        coalesce(
          col("BBFPC_OptOut_Variable_BidAmount"),
          lit(0)
        ) / col("TotalFinalBidPrice")
      )
      .withColumn("BBFPC_OptOut_Fixed_ShareOfBidAmount",
        coalesce(
          col("BBFPC_OptOut_Fixed_BidAmount"),
          lit(0)
        ) / col("TotalFloorPrice")
      )
      .withColumn("BBFPC_OptOut_ShareOfBidAmount",
        coalesce(
          col("BBFPC_OptOut_Variable_ShareOfBidAmount"),
          lit(0)
        ) + coalesce(
          col("BBFPC_OptOut_Fixed_ShareOfBidAmount"),
          lit(0)
        )
      )
      .withColumn("BBFPC_OptOut_ShareOfBids",
        (
          coalesce(
            col("BBFPC_OptOut_Variable_BidCount"),
            lit(0)
          ) + coalesce(
            col("BBFPC_OptOut_Fixed_BidCount"),
            lit(0)
          )
        ) / col("TotalBidCount_includingOptOut")
      )
  }

  case class FilteredCampaignData(CampaignId: String, CampaignType: String)
  case class Campaign(CampaignId: String)

  val CampaignType_NewCampaignNotInThrottleDataset = "NewCampaignNotInThrottleDataset";
  val CampaignType_NewCampaignNotPacing = "NewCampaignNotPacing";
  val CampaignType_AdjustedCampaignPacing = "AdjustedCampaignPacing";
  val CampaignType_AdjustedCampaignNotPacing = "AdjustedCampaignNotPacing";
  val CampaignType_AdjustedCampaignNotInThrottleDataset = "AdjustedCampaignNotInThrottleDataset";
  val CampaignType_AdjustedCampaignNoBids = "AdjustedCampaignNoBids";

  def getFilteredCampaigns(campaignThrottleData: Dataset[CampaignThrottleMetricSchema],
                           potentiallyNewCampaigns: Dataset[Campaign],
                           yesterdaysCampaigns: Dataset[Campaign],
                           underdeliveryThreshold: Double,
                           testSplit: Option[Double]): Dataset[FilteredCampaignData] = {

    val campaignUnderdeliveryData = campaignThrottleData
      .groupBy("CampaignId")
      .agg(
        first($"IsValuePacing").as("IsValuePacing"),
        max($"UnderdeliveryFraction").as("UnderdeliveryFraction")
      )

    val newOrNonSpendingCampaigns = potentiallyNewCampaigns
      .join(campaignUnderdeliveryData, Seq("CampaignId"), "left_anti")
      .join(yesterdaysCampaigns, Seq("CampaignId"), "left_anti")
      .withColumn("CampaignType", lit(CampaignType_NewCampaignNotInThrottleDataset))

    val newUnderDeliveringCampaigns = campaignUnderdeliveryData
      .join(yesterdaysCampaigns, Seq("CampaignId"), "left_anti")
      .filter(col("UnderdeliveryFraction") >= underdeliveryThreshold)
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") < (lit(bucketCount) * testSplit.getOrElse(1.0)) && col("IsValuePacing")) // Filter for Test DA Campaigns only
      .select("CampaignId")
      .withColumn("CampaignType", lit(CampaignType_NewCampaignNotPacing))

    val yesterdaysCampaignsWithCampaignType = yesterdaysCampaigns
      .join(campaignUnderdeliveryData, Seq("CampaignId"), "left")
      .withColumn("CampaignType",
        when(col("UnderdeliveryFraction").isNull, CampaignType_AdjustedCampaignNotInThrottleDataset)
          .when(col("UnderdeliveryFraction") < underdeliveryThreshold, CampaignType_AdjustedCampaignPacing)
          .otherwise(CampaignType_AdjustedCampaignNotPacing)
      )
      .select("CampaignId", "CampaignType")


    val res = newOrNonSpendingCampaigns
      .union(newUnderDeliveringCampaigns)
      .union(yesterdaysCampaignsWithCampaignType)
      .selectAs[FilteredCampaignData]
      .cache()

    res
  }

  def identifyAndHandleProblemCampaigns(
                                         campaignBBFOptOutRate: DataFrame,
                                         yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]
                                       ): (Dataset[CampaignAdjustmentsHadesSchema], Array[(String, Long)]) = {
    val todaysData = campaignBBFOptOutRate
      .withColumn("Hades_isProblemCampaign", col("BBFPC_OptOut_ShareOfBids") > 0.5 && col("HadesBackoff_PCAdjustment") < 1)
      .withColumn("HadesBackoff_PCAdjustment_Current", when(col("Hades_isProblemCampaign"), col("HadesBackoff_PCAdjustment")).otherwise(lit(1)))

    val res = mergeTodayWithYesterdaysData(todaysData, yesterdaysData)
    val metrics = res.filter($"HadesBackoff_PCAdjustment" < 1.0)
      .groupBy("CampaignType")
      .count().as[(String, Long)]
      .collect()

    (res, metrics)
  }

  def mergeTodayWithYesterdaysData(todaysData: DataFrame, yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]) : Dataset[CampaignAdjustmentsHadesSchema] = {
    val oldAdjustments = yesterdaysData
      .drop("HadesBackoff_PCAdjustment_Old", "CampaignType_Yesterday")
      .withColumnRenamed("HadesBackoff_PCAdjustment", "HadesBackoff_PCAdjustment_Old")
      .withColumnRenamed("CampaignType", "CampaignType_Yesterday")
      .select("CampaignId", "HadesBackoff_PCAdjustment_Old", "CampaignType_Yesterday")

    // TODO: If we see all of the old CampaignType_AdjustedCampaignPacing getting proper
    //       adjustments, we could just ignore their previous day's adjustments
    //       and just use the new one. That would make the backoff more dynamic

    todaysData
      .drop("HadesBackoff_PCAdjustment_Old", "CampaignType_Yesterday")
      .join(broadcast(oldAdjustments), Seq("CampaignId") , "outer")
      .withColumn("HadesBackoff_PCAdjustment",
        // We dont want to carry forward yesterday's NewNoUnderdeliveryCampaigns' pushdown as is
        // Since the initial filteredCampaigns list has these campaigns, if the campaign needs a pushdown,
        // It should be calculated in the process.
        when($"CampaignType_Yesterday".isNotNull && $"CampaignType_Yesterday" === CampaignType_NewCampaignNotInThrottleDataset, coalesce($"HadesBackoff_PCAdjustment_Current", lit(1.0)))
        .otherwise(least(coalesce($"HadesBackoff_PCAdjustment_Current", lit(1.0)), coalesce($"HadesBackoff_PCAdjustment_Old", lit(1.0)))))
      .withColumn("Hades_isProblemCampaign", coalesce($"Hades_isProblemCampaign", lit(false)))
      .withColumn("CampaignType", coalesce($"CampaignType", lit(CampaignType_AdjustedCampaignNoBids)))
      .as[CampaignAdjustmentsHadesSchema]
  }

  def getAllBidData(pcOptoutData: Dataset[PlutusLogsData], adGroupData: Dataset[AdGroupRecord], pcResultsMergedData: Dataset[PcResultsMergedDataset]): DataFrame = {

    val adGroupDistinctData = adGroupData.select("AdGroupId", "CampaignId").distinct()

    val plutusLogsData = pcOptoutData
      .filter($"BidBelowFloorExceptedSource" === 2)
      .join(adGroupDistinctData, Seq("AdGroupId"), "inner")
      .drop("AdGroupId", "LegacyPcPushdown", "LogEntryTime")
      .toDF()

    val columns = plutusLogsData.columns

    pcResultsMergedData.drop("LogEntryTime")
      .toDF
      .select(columns.head, columns.drop(1): _*)
      .union(plutusLogsData)
  }

  def transform(date: LocalDate,
                testSplit: Option[Double],
                underdeliveryThreshold: Double,
                yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]
               ): (Dataset[CampaignAdjustmentsHadesSchema], Array[(String, Long)]) = {

    val campaignUnderdeliveryData = loadParquetDataDailyV2[CampaignThrottleMetricSchema](
      CampaignThrottleMetricDataset.S3PATH,
      CampaignThrottleMetricDataset.S3PATH_DATE_GEN,
      date,
      nullIfColAbsent = false // Setting this to false since nullIfColAbsent sets date to null (its a bug with selectAs)
    ).withColumn(
      "Date",
      to_date(col("Date"))
    ).selectAs[CampaignThrottleMetricSchema]

    val pcResultsMergedData = loadParquetDataDailyV2[PcResultsMergedDataset](
      PcResultsMergedDataset.S3_PATH(Some(envForRead)),
      PcResultsMergedDataset.S3_PATH_DATE_GEN,
      date,
      nullIfColAbsent = true
    )

    val pcOptoutData = loadParquetDataDailyV2[PlutusLogsData](
      PlutusLogsDataset.S3PATH_BASE(Some(envForRead)),
      PlutusLogsDataset.S3PATH_DATE_GEN,
      date,
      nullIfColAbsent = true
    )

    // day 1's campaign data is exported at the end of day 0
    val nonArchivedCampaigns = CampaignDataSet().readLatestPartitionUpTo(date.plusDays(1))
      .filter($"IsVisible")
      .select("CampaignId").distinct()

    val liveCampaigns = loadParquetCampaignFlightLatestPartitionUpTo(
      CampaignFlightDataset.S3PATH,
      CampaignFlightDataset.S3PATH_DATE_GEN,
      date.plusDays(1)
    ).filter($"IsCurrent" === 1 && $"StartDateInclusiveUTC" <= date && $"EndDateExclusiveUTC" >= date)
      .join(nonArchivedCampaigns, Seq("CampaignId"), "inner")
      .selectAs[Campaign].distinct()

    val filteredCampaigns = getFilteredCampaigns(
      campaignThrottleData = campaignUnderdeliveryData,
      potentiallyNewCampaigns = liveCampaigns,
      yesterdaysCampaigns = yesterdaysData.selectAs[Campaign],
      underdeliveryThreshold,
      testSplit
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
    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData)

    // Get final pushdowns
    val (res, metrics) = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData)

    (res, metrics)
  }
}