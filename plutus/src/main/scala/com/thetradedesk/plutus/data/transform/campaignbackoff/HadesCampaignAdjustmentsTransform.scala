package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.{envForRead, loadParquetDataDailyV2}
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
  val buffer = 0.8

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

  def getCampaignBidData(pcResultsMerged: Dataset[PcResultsMergedDataset],
                         testSplit: Option[Double]): DataFrame = {

    val gss = udf((m: Double, s: Double, b: Double, e: Double) => gssFunc(m, s, b, e))

    pcResultsMerged
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") < (lit(bucketCount) * testSplit.getOrElse(1.0)) && col("IsValuePacing")) // Filter for Test DA Campaigns only
      .withColumn("Market", // Define Market only using AuctionType and if has DealId
        when(col("DealId").isNotNull,
          when(col("AuctionType").isin(1, 2), "Variable")
            .when(col("AuctionType").isin(3), "Fixed") // This includes PG as well
            .otherwise("Other")
        ).otherwise("OpenMarket"))
      .withColumn("gen_discrepancy", when(col("AuctionType") === 3, lit(1)).otherwise(when(col("Discrepancy") === 0, lit(1)).otherwise(col("Discrepancy"))))
      .withColumn("gen_tensorflowPcModelBid", gss(col("Mu"), col("Sigma"), col("AdjustedBidCPMInUSD"), lit(0.1)))
      .withColumn("gen_plutusPushdown_initial", col("gen_tensorflowPcModelBid") / col("AdjustedBidCPMInUSD"))
      .withColumn("gen_effectiveDiscrepancy", least(lit(1), lit(1) / col("gen_discrepancy")))
      .withColumn("gen_excess", col("gen_effectiveDiscrepancy") - col("gen_plutusPushdown_initial"))
      .withColumn("gen_plutusPushdown",
        when(col("gen_excess") > 0, col("gen_effectiveDiscrepancy") - (col("gen_excess") * when(col("Strategy") === 0, lit(100)).otherwise(col("Strategy")) / lit(100.0)))
          .otherwise(col("gen_effectiveDiscrepancy")))
      .withColumn("gen_proposedBid", col("AdjustedBidCPMInUSD") * col("gen_plutusPushdown"))
      .withColumn("gen_isInBufferZone", col("gen_proposedBid") >= col("FloorPrice") * lit(1 - buffer))
      .withColumn("gen_finalBidPrice", when(col("gen_proposedBid") < col("FloorPrice") && col("gen_isInBufferZone"), col("FloorPrice")).otherwise(col("gen_proposedBid")))
      .withColumn("gen_bufferFloor", (col("FloorPrice") * lit(1 - buffer)))
      .withColumn("gen_plutusPushdownAtBufferFloor", col("gen_bufferFloor") / col("AdjustedBidCPMInUSD"))
      .withColumn("gen_PCAdjustment", (col("gen_effectiveDiscrepancy") - col("gen_plutusPushdownAtBufferFloor")) / (col("gen_effectiveDiscrepancy") - col("gen_plutusPushdown")))
      .withColumn("BidBelowFloorExceptedSource", // Fix BBFSource value for incorrectly marked deal BBF bids that are winning when should be opted out
        when((col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed") && !col("gen_isInBufferZone") && col("MediaCostCPMInUSD").isNotNull), lit(0))
          .otherwise(col("BidBelowFloorExceptedSource")))
      .withColumn("BBFPC_OptOutBid",
        when((col("BidBelowFloorExceptedSource") === 2 && col("Market").isin("Variable", "Fixed") && !col("gen_isInBufferZone")), true)
          .otherwise(false))
  }

  def aggregateCampaignBBFOptOutRate(campaignBidData: DataFrame): DataFrame = {

    val campaignTotalPerformance = campaignBidData
      .groupBy("CampaignId")
      .agg(
        count("*").alias("TotalBidCount_includingOptOut"),
        sum(col("AdvertiserCostInUSD")).as("TotalAdvertiserCostInUSD"),
        sum(col("FinalBidPrice")).as("TotalFinalBidPrice"),
        sum(col("FloorPrice")).as("TotalFloorPrice")
      )

    val campaignBBF_Variable = campaignBidData
      .filter(col("BBFPC_OptOutBid") && col("Market") === "Variable")
      .groupBy("CampaignId")
      .agg(
        sum(col("FinalBidPrice")).alias("BBFPC_OptOut_Variable_BidAmount"),
        count("*").alias("BBFPC_OptOut_Variable_BidCount")
      )

    val campaignBBF_Fixed = campaignBidData
      .filter(col("BBFPC_OptOutBid") && col("Market") === "Fixed")
      .groupBy("CampaignId")
      .agg(
        sum(col("FloorPrice")).alias("BBFPC_OptOut_Fixed_BidAmount"),
        count("*").alias("BBFPC_OptOut_Fixed_BidCount")
      )

    campaignTotalPerformance
      .join(campaignBBF_Variable, Seq("CampaignId"), "left")
      .join(campaignBBF_Fixed, Seq("CampaignId"), "left")
      .withColumn("BBFPC_OptOut_Variable_ShareOfBidAmount", coalesce(col("BBFPC_OptOut_Variable_BidAmount"), lit(0)) / col("TotalFinalBidPrice"))
      .withColumn("BBFPC_OptOut_Fixed_ShareOfBidAmount", coalesce(col("BBFPC_OptOut_Fixed_BidAmount"), lit(0)) / col("TotalFloorPrice"))
      .withColumn("BBFPC_OptOut_ShareOfBidAmount", coalesce(col("BBFPC_OptOut_Variable_ShareOfBidAmount"), lit(0)) + coalesce(col("BBFPC_OptOut_Fixed_ShareOfBidAmount"), lit(0)))
      .withColumn("BBFPC_OptOut_ShareOfBids", (coalesce(col("BBFPC_OptOut_Variable_BidCount"), lit(0)) + coalesce(col("BBFPC_OptOut_Fixed_BidCount"), lit(0))) / col("TotalBidCount_includingOptOut"))
  }

  def identifyAndHandleProblemCampaigns(campaignBBFOptOutRate: DataFrame,
                                        campaignUnderdeliveryData: Dataset[CampaignThrottleMetricSchema],
                                        campaignBidData: DataFrame,
                                        underdeliveryThreshold: Double
                                       ): Dataset[CampaignAdjustmentsHadesSchema] = {

    val optout_condition = col("BBFPC_OptOut_ShareOfBids") > 0.5
    val underdelivery_condition = col("UnderdeliveryFraction") >= underdeliveryThreshold

    val flagProblemCampaigns = campaignBBFOptOutRate
      .join(campaignUnderdeliveryData, Seq("CampaignId"), "left")
      .withColumn("Hades_isProblemCampaign",
        when((optout_condition && underdelivery_condition) || (optout_condition && col("UnderdeliveryFraction").isNull), true).otherwise(false))
      .select("CampaignId", "Hades_isProblemCampaign", "BBFPC_OptOut_ShareOfBids", "BBFPC_OptOut_ShareOfBidAmount") // Don't need Underdelivery because that col already exists when joining back to CampaignBackoff

    val problemCampaigns = flagProblemCampaigns
      .filter(col("Hades_isProblemCampaign"))
      .select("CampaignId")

    val problemCampaigns_campaignPCAdjustment = campaignBidData
      .join(broadcast(problemCampaigns), Seq("CampaignId"))
      .filter(col("FloorPrice") < col("AdjustedBidCPMInUSD")) // Exclude rare cases with initial bids below the floor to avoid skewing the median
      .groupBy("CampaignId")
      .agg(
        expr("percentile_approx(gen_PCAdjustment, 0.5)").as("HadesBackoff_PCAdjustment")
      )

    val getFlagProblemCampaigns = flagProblemCampaigns
      .join(broadcast(problemCampaigns_campaignPCAdjustment), Seq("CampaignId"), "fullouter")

    getFlagProblemCampaigns.selectAs[CampaignAdjustmentsHadesSchema]
  }

  def transform(date: LocalDate, testSplit: Option[Double], underdeliveryThreshold: Double): Dataset[CampaignAdjustmentsHadesSchema] = {

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

    val campaignBidData = getCampaignBidData(pcResultsMergedData, testSplit)

    val campaignBBFOptOutRate = aggregateCampaignBBFOptOutRate(campaignBidData)

    identifyAndHandleProblemCampaigns(campaignBBFOptOutRate, campaignUnderdeliveryData, campaignBidData, underdeliveryThreshold)

  }


}