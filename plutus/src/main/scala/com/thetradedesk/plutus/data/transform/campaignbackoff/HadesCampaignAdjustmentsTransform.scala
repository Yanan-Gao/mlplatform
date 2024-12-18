package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.{AuctionType, envForRead, loadParquetDataDailyV2}
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignAdjustmentsHadesSchema, CampaignThrottleMetricDataset, CampaignThrottleMetricSchema}
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import org.apache.hadoop.shaded.org.apache.commons.math3.special.Erf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import java.time.LocalDate
import scala.util.hashing.MurmurHash3

object HadesCampaignAdjustmentsTransform {

  // Constants
  val bucketCount = 1000
  val buffer = 0.75

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

  def getUnderdeliveringCampaignBidData(pcResultsMerged: Dataset[PcResultsMergedDataset],
                                        underdeliveringCampaigns: Array[String]): DataFrame = {

    val gss = udf((m: Double, s: Double, b: Double, e: Double) => gssFunc(m, s, b, e))

    pcResultsMerged
      .filter(col("FloorPrice") < col("AdjustedBidCPMInUSD")) // Exclude rare cases with initial bids below the floor to avoid skewing the median
      .filter(col("CampaignId").isin(underdeliveringCampaigns: _*))
      .withColumn("Market", // Define Market only using AuctionType and if has DealId
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(AuctionType.FirstPrice, AuctionType.SecondPrice), "Variable")
            .when(col("AuctionType").isin(AuctionType.FixedPrice), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      .withColumn("gen_discrepancy", when(col("AuctionType") === AuctionType.FixedPrice, lit(1)).otherwise(when(col("Discrepancy") === 0, lit(1)).otherwise(col("Discrepancy"))))
      .withColumn("gen_gss_pushdown", when(col("AuctionType") =!= AuctionType.FixedPrice, $"GSS").otherwise(gss(col("Mu"), col("Sigma"), col("AdjustedBidCPMInUSD"), lit(0.1)) / col("AdjustedBidCPMInUSD")))
      .withColumn("gen_effectiveDiscrepancy", least(lit(1), lit(1) / col("gen_discrepancy")))
      .withColumn("gen_excess", col("gen_effectiveDiscrepancy") - col("gen_gss_pushdown"))
      .filter(col("gen_excess") > 0)
      .withColumn("gen_bufferFloor", (col("FloorPrice") * lit(1 - buffer)))
      .withColumn("gen_plutusPushdownAtBufferFloor", col("gen_bufferFloor") / col("AdjustedBidCPMInUSD"))

      .withColumn("gen_PCAdjustment", (col("gen_effectiveDiscrepancy") - col("gen_plutusPushdownAtBufferFloor")) / (col("gen_effectiveDiscrepancy") - col("gen_gss_pushdown")))

      .withColumn("gen_plutusPushdown", col("gen_effectiveDiscrepancy") - (col("gen_excess") * when(col("Strategy") === 0, lit(100)).otherwise(col("Strategy")) / lit(100.0)))
      .withColumn("gen_proposedBid", col("AdjustedBidCPMInUSD") * col("gen_plutusPushdown"))
      .withColumn("gen_isInBufferZone", col("gen_proposedBid") >= col("gen_bufferFloor"))
      .withColumn("BidBelowFloorExceptedSource", // Fix BBFSource value for incorrectly marked deal BBF bids that are winning when should be opted out
        when((col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed") && !col("gen_isInBufferZone") && col("MediaCostCPMInUSD").isNotNull), lit(0))
          .otherwise(col("BidBelowFloorExceptedSource")))
      .withColumn("BBFPC_OptOutBid",
        when((col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed") && !col("gen_isInBufferZone")), true)
          .otherwise(false))
  }

  def aggregateCampaignBBFOptOutRate(campaignBidData: DataFrame): DataFrame = {
    campaignBidData
      .groupBy("CampaignId")
      .agg(
        count("*").as("TotalBidCount_includingOptOut"),
        sum(col("AdvertiserCostInUSD")).as("TotalAdvertiserCostInUSD"),
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

  def getUnderdeliveringCampaignsInTestBucket(campaignUnderdeliveryData: Dataset[CampaignThrottleMetricSchema], underdeliveryThreshold: Double, testSplit: Option[Double]): Array[String] = {
    campaignUnderdeliveryData
      .filter(col("UnderdeliveryFraction") >= underdeliveryThreshold)
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") < (lit(bucketCount) * testSplit.getOrElse(1.0)) && col("IsValuePacing")) // Filter for Test DA Campaigns only
      .select("CampaignId")
      .collect()
      .map(_.getString(0))
      .distinct
  }

  def identifyAndHandleProblemCampaigns(campaignBBFOptOutRate: DataFrame, yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]): Dataset[CampaignAdjustmentsHadesSchema] = {
    val todaysData = campaignBBFOptOutRate
      .withColumn("Hades_isProblemCampaign", col("BBFPC_OptOut_ShareOfBids") > 0.5 && col("HadesBackoff_PCAdjustment") < 1)
      .withColumn("HadesBackoff_PCAdjustment_Current", when(col("Hades_isProblemCampaign"), col("HadesBackoff_PCAdjustment")).otherwise(lit(1)))

    mergeTodayWithYesterdaysData(todaysData, yesterdaysData)
  }

  def mergeTodayWithYesterdaysData(todaysData: DataFrame, yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]) : Dataset[CampaignAdjustmentsHadesSchema] = {
    val oldAdjustments = yesterdaysData
      .drop("HadesBackoff_PCAdjustment_Old")
      .withColumnRenamed("HadesBackoff_PCAdjustment", "HadesBackoff_PCAdjustment_Old")
      .select("CampaignId", "HadesBackoff_PCAdjustment_Old")

    todaysData
      .drop("HadesBackoff_PCAdjustment_Old")
      .join(broadcast(oldAdjustments.alias("yesterday")), Seq("CampaignId") , "outer")
      .withColumn("HadesBackoff_PCAdjustment", least(coalesce($"HadesBackoff_PCAdjustment_Current", lit(1.0)), coalesce($"HadesBackoff_PCAdjustment_Old", lit(1.0))))
      .withColumn("Hades_isProblemCampaign", coalesce($"Hades_isProblemCampaign", lit(false)))
      .as[CampaignAdjustmentsHadesSchema]
  }

  def transform(date: LocalDate, testSplit: Option[Double], underdeliveryThreshold: Double, yesterdaysData: Dataset[CampaignAdjustmentsHadesSchema]): (Dataset[CampaignAdjustmentsHadesSchema], Int) = {

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

    val underdeliveringCampaigns = getUnderdeliveringCampaignsInTestBucket(campaignUnderdeliveryData, underdeliveryThreshold, testSplit)

    val campaignBidData = getUnderdeliveringCampaignBidData(pcResultsMergedData, underdeliveringCampaigns)

    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData)

    val res = identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, yesterdaysData)

    (res, underdeliveringCampaigns.length)
  }
}