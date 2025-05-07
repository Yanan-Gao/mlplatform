package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.plutus.data.envForReadInternal
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.plutus.data.schema.campaignbackoff.{CampaignFlightDataset, CampaignFloorBufferDataset, CampaignFloorBufferRollbackDataset, CampaignFloorBufferSchema, CampaignThrottleMetricDataset, CampaignThrottleMetricSchema, RtbPlatformReportCondensedData}
import com.thetradedesk.plutus.data.transform.campaignbackoff.HadesCampaignAdjustmentsTransform.{Campaign, bucketCount, getTestBucketUDF}
import com.thetradedesk.plutus.data.utils.S3NoFilesFoundException
import com.thetradedesk.spark.datasets.sources.SupplyVendorDataSet
import com.thetradedesk.spark.datasets.sources.vertica.UnifiedRtbPlatformReportDataSet
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.TTDConfig.config
import job.campaignbackoff.CampaignBbfFloorBufferCandidateSelectionJob.{floorBufferMetrics, numFloorBufferRollbackRowsWritten, numFloorBufferRowsWritten}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.LocalDate

object CampaignFloorBufferCandidateSelectionTransform {
  val testSplit = config.getDoubleOption("testSplit")

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
  case class FlightData(CampaignId: String, EndDateExclusiveUTC: Timestamp)
  case class PacingData(CampaignId: String, IsValuePacing: Boolean)
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
                                   flightData: Dataset[FlightData],
                                   pacingData: Dataset[PacingData],
                                   campaignUnderDeliveryData: Dataset[CampaignUnderDeliveryData],
                                   latestRollbackBufferSnapshot: Dataset[CampaignFloorBufferSchema],
                                   testSplit: Option[Double],
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
      .join(pacingData, Seq("CampaignId"), "inner")
      .join(flightData, Seq("CampaignId"), "inner")
      .join(supplyVendorData, Seq("SupplyVendor"), "left")

    val aggregatedRtbPlatform = rtbSupplyVendorPacingJoined
      .withColumn("TestBucket", getTestBucketUDF(col("CampaignId"), lit(bucketCount)))
      .filter(col("TestBucket") < (lit(bucketCount) * testSplit.getOrElse(1.0)))
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

    val onePercentFloorBufferEligibleCandidates = candidatesWithOnePercentCriteria
      .select("CampaignId")
      .distinct()
      .withColumn("BBF_FloorBuffer", lit(onePercentFloorBufferCriteria.floorBuffer))
      .withColumn("AddedDate", lit(date))
      .selectAs[CampaignFloorBufferSchema]
      .distinct()

    // Remove any campaigns that were rolled back within 7 days
    val rollbackThresholdDate = date.minusDays(7)
    val recentlyRolledBackCandidates = latestRollbackBufferSnapshot.filter(col("AddedDate") > rollbackThresholdDate).selectAs[Campaign].distinct()
    val eligibleCandidates = onePercentFloorBufferEligibleCandidates.join(broadcast(recentlyRolledBackCandidates), Seq("CampaignId"), "left_anti").selectAs[CampaignFloorBufferSchema]

    eligibleCandidates
  }

  def getRollbackCampaigns(
                            date: LocalDate,
                            yesterdaysCampaignFloorBufferSnapshot: Dataset[CampaignFloorBufferSchema],
                            todaysCampaignThrottleMetricData: Dataset[CampaignThrottleMetricSchema],
                            onePercentFloorBufferRollbackCriteria: OnePercentFloorBufferRollbackCriteria
                                ): Dataset[CampaignFloorBufferSchema] = {
    /*
      Rollback criteria:
        Underdelivery fraction <0.05
        Throttle <= 0.8
      If it doesn't hit this criteria, then rollback to platform-wide buffer.
     */

    val rollbackEligibleCandidatesOnePercent = yesterdaysCampaignFloorBufferSnapshot
      .join(todaysCampaignThrottleMetricData
        .filter(col("UnderdeliveryFraction") >= onePercentFloorBufferRollbackCriteria.rollbackUnderdeliveryFraction &&
          col("CampaignThrottleMetric") > onePercentFloorBufferRollbackCriteria.rollbackCampaignThrottleMetric), Seq("CampaignId"), "inner")
      .withColumn("AddedDate", lit(date))

    rollbackEligibleCandidatesOnePercent.selectAs[CampaignFloorBufferSchema]
  }

  def generateTodaysSnapshot(
                              yesterdaysCampaignFloorBufferSnapshot: Dataset[CampaignFloorBufferSchema],
                              todaysCandidatesWithFloorBuffer: Dataset[CampaignFloorBufferSchema],
                              todaysRollbackEligibleCampaigns: Dataset[CampaignFloorBufferSchema]
                            ): Dataset[CampaignFloorBufferSchema] = {
    val yesterdaysCampaignFloorBufferSnapshotRenamed = yesterdaysCampaignFloorBufferSnapshot
      .withColumnRenamed("BBF_FloorBuffer", "LatestBBF_FloorBuffer")
      .withColumnRenamed("AddedDate", "LatestBBF_AddedDate")
    val todaysCandidatesWithFloorBufferRenamed = todaysCandidatesWithFloorBuffer
      .withColumnRenamed("BBF_FloorBuffer", "TodaysBBF_FloorBuffer")
      .withColumnRenamed("AddedDate", "TodaysBBF_AddedDate")
    val campaignsToRollback = todaysRollbackEligibleCampaigns.selectAs[Campaign]

    val todaysCampaignFloorBufferSnapshot = yesterdaysCampaignFloorBufferSnapshotRenamed
      .join(broadcast(todaysCandidatesWithFloorBufferRenamed), Seq("CampaignId"), "outer")
      .select(
        col("CampaignId"),
        coalesce(col("TodaysBBF_FloorBuffer"), col("LatestBBF_FloorBuffer")).as("BBF_FloorBuffer"),
        coalesce(col("TodaysBBF_AddedDate"), col("LatestBBF_AddedDate")).as("AddedDate")
      )
      .join(campaignsToRollback, Seq("CampaignId"), "left_anti")
      .selectAs[CampaignFloorBufferSchema]
      .cache()

    todaysCampaignFloorBufferSnapshot
  }

  private def getCampaignFloorBufferSnapshot(date: LocalDate,
                                                          onePercentFloorBufferTestCriteria: OnePercentFloorBufferCriteria,
                                                          onePercentFloorBufferRollbackCriteria: OnePercentFloorBufferRollbackCriteria
                                                         ) = {

    // Read the latest floor buffer snapshot
    val yesterdaysCampaignFloorBufferSnapshot = CampaignFloorBufferDataset.readLatestDataUpToIncluding(date, envForReadInternal)
      .distinct()

    // Read the latest floor buffer rollback snapshot
    val latestRollbackBufferSnapshot = try {
      CampaignFloorBufferRollbackDataset.readLatestDataUpToIncluding(date, envForReadInternal)
      .distinct()
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
    val flightData = CampaignFlightDataset.readDate(date, env=envForReadInternal)
      .selectAs[FlightData]
      .distinct()
      .filter(col("EndDateExclusiveUTC") >= to_date(lit(minFlightEnd)))

    val pacingData = CampaignThrottleMetricDataset.readDate(date, envForReadInternal)
      .selectAs[PacingData]
      .filter(col("IsValuePacing"))

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
      onePercentFloorBufferTestCriteria,
      platformReportData,
      supplyVendorData,
      flightData,
      pacingData,
      campaignUnderDeliveryData,
      latestRollbackBufferSnapshot,
      testSplit
    )

    // Remove the campaigns that fall under rollback logic
    val rollbackEligibleCampaigns = getRollbackCampaigns(
      date,
      yesterdaysCampaignFloorBufferSnapshot,
      todaysCampaignThrottleMetricData,
      onePercentFloorBufferRollbackCriteria
    )

    // TODO: Change this to handle different floor buffer criterias
    val campaignBufferFloorSnapshot = generateTodaysSnapshot(
      yesterdaysCampaignFloorBufferSnapshot,
      candidateCampaigns,
      rollbackEligibleCampaigns)

    (rollbackEligibleCampaigns, campaignBufferFloorSnapshot)
  }

  def generateBbfFloorBufferMetrics(
                       todaysCampaignFloorBufferDatasetSnapshot: Dataset[CampaignFloorBufferSchema],
                       todaysRollbackEligibleCampaigns: Dataset[CampaignFloorBufferSchema]
                     ) = {
    val floorBufferCounts = todaysCampaignFloorBufferDatasetSnapshot.groupBy("BBF_FloorBuffer").count()
    BbfFloorBufferMetrics(
      floorBufferTotalCount = todaysCampaignFloorBufferDatasetSnapshot.count(),
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
               ): Unit = {

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

    // Generate today's buffer floor snapshot
    val (todaysRollbackEligibleCampaigns, todaysCampaignFloorBufferDatasetSnapshot) = getCampaignFloorBufferSnapshot(
      date=date,
      onePercentFloorBufferTestCriteria=onePercentFloorBufferCriteria,
      onePercentFloorBufferRollbackCriteria=onePercentFloorBufferRollbackCriteria
    )

    // Write the buffer floor snapshot and rollback snapshot to S3
    CampaignFloorBufferDataset.writeData(date = date, dataset = todaysCampaignFloorBufferDatasetSnapshot, filecount = fileCount)
    CampaignFloorBufferRollbackDataset.writeData(date = date, dataset = todaysRollbackEligibleCampaigns, filecount = fileCount)

    // Write metrics
    val floorBufferJobMetrics = generateBbfFloorBufferMetrics(
      todaysCampaignFloorBufferDatasetSnapshot,
      todaysRollbackEligibleCampaigns
    )

    floorBufferJobMetrics.countByFloorBuffer.collect().foreach { row =>
      val bufferFloorValue = row.getAs[Double]("BBF_FloorBuffer")
      val countValue = row.getAs[Long]("count")
      floorBufferMetrics.labels(bufferFloorValue.toString).set(countValue)
    }
    numFloorBufferRowsWritten.set(floorBufferJobMetrics.floorBufferTotalCount)
    numFloorBufferRollbackRowsWritten.set(floorBufferJobMetrics.floorBufferRollbackTotalCount)
  }
}
