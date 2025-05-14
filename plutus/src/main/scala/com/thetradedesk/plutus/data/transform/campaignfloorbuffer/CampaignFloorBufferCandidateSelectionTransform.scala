package com.thetradedesk.plutus.data.transform.campaignfloorbuffer

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.schema.campaignfloorbuffer.{CampaignFloorBufferDataset, CampaignFloorBufferRollbackDataset, CampaignFloorBufferSchema}
import com.thetradedesk.plutus.data.schema.shared.BackoffCommon.{Campaign, CampaignFlightData}
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.SupplyVendorDataSet
import com.thetradedesk.spark.datasets.sources.vertica.UnifiedRtbPlatformReportDataSet
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionJob.{floorBufferMetrics, numFloorBufferRollbackRowsWritten, numFloorBufferRowsWritten}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate

object CampaignFloorBufferCandidateSelectionTransform {

  case class OnePercentFloorBufferCriteria(
                                            avgUnderdeliveryFraction: Double,
                                            avgCampaignThrottleMetric: Double,
                                            openMarketShare: Double,
                                            openPathShare: Double,
                                            floorBuffer: Double
                                          )

  case class OnePercentFloorBufferRollbackCriteria(
                                            rollbackUnderdeliveryFraction: Double,
                                            rollbackCampaignThrottleMetric: Double
                                          )

  case class BbfFloorBufferMetrics(
                                    floorBufferTotalCount: Double,
                                    floorBufferRollbackTotalCount: Double,
                                    countByFloorBuffer: DataFrame
                                  )

  case class SupplyVendorData(SupplyVendor: String, OpenPathEnabled: Boolean)
  case class CampaignUnderDeliveryData(
                                        CampaignId: String,
                                        AvgUnderdeliveryInUSD: Double,
                                        AvgUnderdeliveryFraction: Double,
                                        AvgCampaignThrottleMetric: Double,
                                        AvgEffectiveKeepRate: Double
                                      )

  def getFloorBufferCandidateCampaigns(
                                        date: LocalDate,
                                        onePercentFloorBufferCriteria: OnePercentFloorBufferCriteria,
                                        platformReportData: Dataset[RtbPlatformReportCondensedData],
                                        supplyVendorData: Dataset[SupplyVendorData],
                                        flightData: Dataset[CampaignFlightData],
                                        campaignUnderDeliveryData: Dataset[CampaignUnderDeliveryData],
                                        latestRollbackBufferData: Dataset[CampaignFloorBufferSchema],
                                 ): Dataset[CampaignFloorBufferSchema] = {
    /*
     Run query for today's candidate selection (currently query only for 1% buffer)
      - Hades Test Buckets Only
      - Underdelivery fraction <0.02
      - Throttle <= 0.8
      - OM share >= 0.01
      - OP share >= 0.01
      - Rollback date is 7 days prior or no rollback date set.
     */

    val rtbSupplyVendorPacingJoined = platformReportData
      .join(flightData, Seq("CampaignId"), "inner")
      .join(supplyVendorData, Seq("SupplyVendor"), "left")

    val aggregatedRtbPlatform = rtbSupplyVendorPacingJoined
      .withColumn("IsPrivate", when(col("PrivateContractId").isNull, "Open").otherwise("Private"))
      .groupBy("CampaignId", "OpenPathEnabled", "IsPrivate")
      .agg(sum(col("PartnerCostInUSD")).alias("PartnerCostInUSD"))

    val openMarketOpenPathShare = aggregatedRtbPlatform
      .groupBy("CampaignId")
      .agg(
        sum("PartnerCostInUSD").alias("TotalPartnerCost"),
        sum(
          when(col("IsPrivate") === "Open", col("PartnerCostInUSD"))
            .otherwise(lit(0))
        ).as("OpenMarketCost"),
        sum(
          when(col("OpenPathEnabled") === true, col("PartnerCostInUSD"))
            .otherwise(lit(0))
        ).as("OpenPathCost")
      )
      .withColumn("OpenMarketShare", coalesce(col("OpenMarketCost") / col("TotalPartnerCost"), lit(0.0)))
      .withColumn("OpenPathShare", coalesce(col("OpenPathCost") / col("TotalPartnerCost"), lit(0.0)))
      .select("CampaignId", "OpenMarketShare", "OpenPathShare")

    // Final joined dataframe
    val candidatesWithOnePercentCriteria = campaignUnderDeliveryData
      .join(openMarketOpenPathShare, Seq("CampaignId"), "inner")
      .filter(
        col("AvgUnderdeliveryFraction") <= onePercentFloorBufferCriteria.avgUnderdeliveryFraction
          && col("AvgCampaignThrottleMetric") <= onePercentFloorBufferCriteria.avgCampaignThrottleMetric
          && col("OpenMarketShare") >= onePercentFloorBufferCriteria.openMarketShare
          && col("OpenPathShare") >= onePercentFloorBufferCriteria.openPathShare
      )

    val onePercentFloorBufferEligibleCampaigns = candidatesWithOnePercentCriteria
      .select("CampaignId")
      .distinct()
      .withColumn("BBF_FloorBuffer", lit(onePercentFloorBufferCriteria.floorBuffer))
      .withColumn("AddedDate", lit(date))
      .selectAs[CampaignFloorBufferSchema]
      .distinct()

    // Remove any campaigns that were rolled back within 7 days
    val rollbackThresholdDate = date.minusDays(7)
    val recentlyRolledBackCampaigns = latestRollbackBufferData.filter(col("AddedDate") > rollbackThresholdDate).selectAs[Campaign].distinct()
    val eligibleCampaigns = onePercentFloorBufferEligibleCampaigns.join(broadcast(recentlyRolledBackCampaigns), Seq("CampaignId"), "left_anti").selectAs[CampaignFloorBufferSchema]

    eligibleCampaigns
  }

  def getRollbackCampaigns(
                            date: LocalDate,
                            yesterdaysCampaignFloorBufferData: Dataset[CampaignFloorBufferSchema],
                            todaysCampaignThrottleMetricData: Dataset[CampaignThrottleMetricSchema],
                            onePercentFloorBufferRollbackCriteria: OnePercentFloorBufferRollbackCriteria
                                ): Dataset[CampaignFloorBufferSchema] = {
    /*
      Rollback criteria:
        Underdelivery fraction <0.05
        Throttle <= 0.8
      If it doesn't hit this criteria, then rollback to platform-wide buffer.
     */

    val rollbackEligibleCampaignsOnePercent = yesterdaysCampaignFloorBufferData
      .join(todaysCampaignThrottleMetricData
        .filter(col("UnderdeliveryFraction") >= onePercentFloorBufferRollbackCriteria.rollbackUnderdeliveryFraction &&
          col("CampaignThrottleMetric") > onePercentFloorBufferRollbackCriteria.rollbackCampaignThrottleMetric), Seq("CampaignId"), "inner")
      .withColumn("AddedDate", lit(date))

    rollbackEligibleCampaignsOnePercent.selectAs[CampaignFloorBufferSchema]
  }

  def generateTodaysFloorBufferDataset(
                                        yesterdaysCampaignFloorBufferData: Dataset[CampaignFloorBufferSchema],
                                        todaysCandidatesWithFloorBuffer: Dataset[CampaignFloorBufferSchema],
                                        todaysRollbackEligibleCampaigns: Dataset[CampaignFloorBufferSchema]
                            ): Dataset[CampaignFloorBufferSchema] = {
    val yesterdaysCampaignFloorBufferDataRenamed = yesterdaysCampaignFloorBufferData
      .withColumnRenamed("BBF_FloorBuffer", "LatestBBF_FloorBuffer")
      .withColumnRenamed("AddedDate", "LatestBBF_AddedDate")
    val todaysCandidatesWithFloorBufferRenamed = todaysCandidatesWithFloorBuffer
      .withColumnRenamed("BBF_FloorBuffer", "TodaysBBF_FloorBuffer")
      .withColumnRenamed("AddedDate", "TodaysBBF_AddedDate")
    val campaignsToRollback = todaysRollbackEligibleCampaigns.selectAs[Campaign]

    val todaysCampaignFloorBufferData = yesterdaysCampaignFloorBufferDataRenamed
      .join(broadcast(todaysCandidatesWithFloorBufferRenamed), Seq("CampaignId"), "outer")
      .select(
        col("CampaignId"),
        coalesce(col("TodaysBBF_FloorBuffer"), col("LatestBBF_FloorBuffer")).as("BBF_FloorBuffer"),
        coalesce(col("TodaysBBF_AddedDate"), col("LatestBBF_AddedDate")).as("AddedDate")
      )
      .join(campaignsToRollback, Seq("CampaignId"), "left_anti")
      .selectAs[CampaignFloorBufferSchema]
      .cache()

    todaysCampaignFloorBufferData
  }

  def generateBbfFloorBufferMetrics(
                                     todaysCampaignFloorBufferDataset: Dataset[CampaignFloorBufferSchema],
                                     todaysRollbackEligibleCampaigns: Dataset[CampaignFloorBufferSchema]
                     ) = {
    val floorBufferCounts = todaysCampaignFloorBufferDataset.groupBy("BBF_FloorBuffer").count()
    BbfFloorBufferMetrics(
      floorBufferTotalCount = todaysCampaignFloorBufferDataset.count(),
      floorBufferRollbackTotalCount = todaysRollbackEligibleCampaigns.count(),
      countByFloorBuffer = floorBufferCounts
    )
  }

  def transform(date: LocalDate,
                fileCount: Int,
                underdeliveryFraction: Double,
                throttleThreshold: Double,
                openMarketShare: Double,
                openPathShare: Double,
                rollbackUnderdeliveryFraction: Double,
                rollbackThrottleThreshold: Double,
                liveHadesTestDACampaigns: Dataset[CampaignFlightData]
               ): Dataset[CampaignFloorBufferSchema] = {

    // Currently there is only 1% buffer floor criteria. We can create another criteria class to handle multiple criterias
    val onePercentFloorBufferCriteria = OnePercentFloorBufferCriteria(
      avgUnderdeliveryFraction=underdeliveryFraction,
      avgCampaignThrottleMetric=throttleThreshold,
      openMarketShare=openMarketShare,
      openPathShare=openPathShare,
      floorBuffer = 0.01
    )

    val onePercentFloorBufferRollbackCriteria = OnePercentFloorBufferRollbackCriteria(
      rollbackUnderdeliveryFraction = rollbackUnderdeliveryFraction,
      rollbackCampaignThrottleMetric = rollbackThrottleThreshold,
    )

    // Read the latest floor buffer data
    val yesterdaysCampaignFloorBufferData = CampaignFloorBufferDataset.readLatestDataUpToIncluding(date, envForReadInternal)
      .distinct()

    // Read the latest floor buffer rollback data
    val latestRollbackBufferData = try {
      CampaignFloorBufferRollbackDataset.readLatestDataUpToIncluding(date, envForReadInternal).distinct()
    } catch {
      case _: S3NoFilesFoundException => Seq.empty[CampaignFloorBufferSchema].toDS()
    }

    // Read RtbPlatformReportCondensedData for yesterday and day before yesterday
    val platformReportData = UnifiedRtbPlatformReportDataSet
      .readRange(date.minusDays(2), date, isInclusive = true)
      .selectAs[RtbPlatformReportCondensedData]

    // Read supply vendor data
    val supplyVendorData = SupplyVendorDataSet()
      .readLatestPartitionUpTo(date)
      .withColumn("SupplyVendor", lower(col("SupplyVendorName")))
      .selectAs[SupplyVendorData]

    // Read flight data
    val minFlightEnd: LocalDate = date.plusDays(2)
    val flightData = liveHadesTestDACampaigns
      .filter(col("EndDateExclusiveUTC") >= to_date(lit(minFlightEnd)))

    val todaysCampaignThrottleMetricData = CampaignThrottleMetricDataset.readDate(date, envForReadInternal)
      .selectAs[CampaignThrottleMetricSchema]

    val campaignUnderDeliveryData = CampaignThrottleMetricDataset
      .readLatestDataUpToIncluding(date, lookBack = 6, env = envForReadInternal)
      .groupBy("CampaignId").agg(
        avg(col("UnderdeliveryInUSD")).alias("AvgUnderdeliveryInUSD"),
        avg(col("UnderdeliveryFraction")).alias("AvgUnderdeliveryFraction"),
        avg(col("CampaignThrottleMetric")).alias("AvgCampaignThrottleMetric"),
        avg(col("CampaignEffectiveKeepRate")).alias("AvgEffectiveKeepRate")
      ).selectAs[CampaignUnderDeliveryData]

    // Run query for today's candidate selection (currently querying only for 1% buffer)
    val candidateCampaigns = getFloorBufferCandidateCampaigns(
      date,
      onePercentFloorBufferCriteria,
      platformReportData,
      supplyVendorData,
      flightData,
      campaignUnderDeliveryData,
      latestRollbackBufferData
    )

    // Remove the campaigns that fall under rollback logic
    val rollbackEligibleCampaigns = getRollbackCampaigns(
      date,
      yesterdaysCampaignFloorBufferData,
      todaysCampaignThrottleMetricData,
      onePercentFloorBufferRollbackCriteria
    )

    // Generate today's data
    val campaignBufferFloorData = generateTodaysFloorBufferDataset(
      yesterdaysCampaignFloorBufferData,
      candidateCampaigns,
      rollbackEligibleCampaigns)

    // Write the buffer floor data and rollback dataset to S3
    CampaignFloorBufferDataset.writeData(date = date, dataset = campaignBufferFloorData, filecount = fileCount)
    CampaignFloorBufferRollbackDataset.writeData(date = date, dataset = rollbackEligibleCampaigns, filecount = fileCount)

    // Write metrics
    val floorBufferJobMetrics = generateBbfFloorBufferMetrics(
      campaignBufferFloorData,
      rollbackEligibleCampaigns
    )

    floorBufferJobMetrics.countByFloorBuffer.collect().foreach { row =>
      val bufferFloorValue = row.getAs[Double]("BBF_FloorBuffer")
      val countValue = row.getAs[Long]("count")
      floorBufferMetrics.labels(bufferFloorValue.toString).set(countValue)
    }
    numFloorBufferRowsWritten.set(floorBufferJobMetrics.floorBufferTotalCount)
    numFloorBufferRollbackRowsWritten.set(floorBufferJobMetrics.floorBufferRollbackTotalCount)
    campaignBufferFloorData
  }
}
