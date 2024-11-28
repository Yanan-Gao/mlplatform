package com.thetradedesk.plutus.data.transform.campaignbackoff

import com.thetradedesk.logging.Logger
import com.thetradedesk.plutus.data.schema.PcResultsMergedDataset
import com.thetradedesk.plutus.data.schema.campaignbackoff.CampaignFlightDataset.{loadParquetCampaignFlight, loadParquetCampaignFlightLatestPartitionUpTo}
import com.thetradedesk.plutus.data.schema.campaignbackoff._
import com.thetradedesk.plutus.data.transform.SharedTransforms.AddChannel
import com.thetradedesk.plutus.data.{envForRead, envForWrite, loadParquetDataDailyV2}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.datasets.sources.vertica.UnifiedRtbPlatformReportDataSet
import com.thetradedesk.spark.datasets.sources.{AdFormatDataSet, AdFormatRecord, CountryDataSet, CountryRecord}
import com.thetradedesk.spark.listener.WriteListener
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import job.campaignbackoff.CampaignAdjustmentsJob.{campaignCounts, numRowsWritten, testControlSplit}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.time.LocalDate

object CampaignAdjustmentsTransform extends Logger {
  // Constants
  val numPartitions = 200

  val checkForAggressiveFPA = 0.65
  val checkMinBidCount = 500

  val maxPacingUnderdeliveryFraction = 0.02
  val valCloseTo0 = 1e-10
  val improvedPacingThresholdPct = -15

  val defaultAdjustment = 1.0
  val stepSizeAdjustment = 0.1
  val aggressiveStepSizeAdjustment = 0.15
  val minAdjustment = 0.2

  def getUnderdelivery(campaignUnderdeliveryData: Dataset[CampaignThrottleMetricSchema], //campaignUnderdeliveryData: Dataset[CampaignUnderdeliveryRecord],
                       campaignFlightData: Dataset[CampaignFlightRecord],
                       date: LocalDate
                      ): (DataFrame, DataFrame) = {

    // Get campaign performance data from past 5 days of data to determine whether underdelivering
    val campaignUnderdelivery = campaignUnderdeliveryData
      .repartition(numPartitions, col("CampaignId"))

    // Collect campaign details
    val campaignInfo5days = campaignUnderdelivery.alias("ud")
      .join(broadcast(campaignFlightData.alias("cf")), col("ud.CampaignId") === col("cf.CampaignId") && col("ud.CampaignFlightId") === col("cf.CampaignFlightId"), "inner")
      .filter(col("cf.IsCurrent") === 1)
      .select(col("ud.*"), col("cf.EndDateExclusiveUTC"), col("cf.IsCurrent"))

    val underdeliveringCampaigns = isUnderdelivering(campaignInfo5days, date)

    // Get past 2 days of campaign pacing data. If campaign ended, there will only be 1 day
    val checkPacingFilterDate = date.minusDays(1)
    val campaignInfo2days = campaignInfo5days.filter(col("Date") >= checkPacingFilterDate)
    campaignInfo2days.cache()

    (campaignInfo2days, underdeliveringCampaigns)
  }

  def getUnderdeliveringDueToPlutus(underdeliveringCampaigns: DataFrame,
                                    platformReportData: Dataset[RtbPlatformReportCondensedData],
                                    countryData: Dataset[CountryRecord],
                                    adFormatData: Dataset[AdFormatRecord],
                                    platformWideStatsData: Dataset[PlatformWideStatsSchema],
                                    pcResultsMergedData: Dataset[PcResultsMergedDataset]
                                   ): DataFrame = {

    val rtb_agg = joinCampaignsToPlatformReportAndAggregate(underdeliveringCampaigns, platformReportData, countryData, adFormatData)

    val compareStats = joinToPlatformWideStatsAndCompareRelativeStats(rtb_agg, platformWideStatsData)

    val platWide_underdeliveringCampaigns = filterPlausiblePC(compareStats, checkForAggressiveFPA, checkMinBidCount)

    val check_discrepancy_pc = checkDiscrepancyPC(pcResultsMergedData, platWide_underdeliveringCampaigns)

    val newPCUnderdeliveringCampaigns = check_discrepancy_pc.select("CampaignId").distinct()

    underdeliveringCampaigns.unpersist()

    newPCUnderdeliveringCampaigns.cache()
  }

  def updateAdjustments(campaignInfo2days: DataFrame,
                        date: LocalDate,
                        newPCUnderdeliveringCampaigns: DataFrame,
                        campaignAdjustmentsPacingData: Dataset[CampaignAdjustmentsPacingSchema],
                        testSplit: Option[Double],
                        updateAdjustmentsVersion: String
                       ): Dataset[CampaignAdjustmentsPacingSchema] = {

    /*** Update previously adjusted campaigns ***/

    // Filter previously adjusted campaigns
    val prevAdjustedCampaigns_campaignAdjustmentsData = campaignAdjustmentsPacingData
      .filter(
        col("IsTest") && // Only test campaigns
          col("CampaignPCAdjustment") =!= defaultAdjustment // Exclude previously reset campaigns
      )

    val distinct_prevAdjustedCampaigns = prevAdjustedCampaigns_campaignAdjustmentsData
      .select("CampaignId").distinct()

    // Get last 2 days of spend and flight data for previously adjusted campaigns to use for percent change calculation
    val prevAdjustedCampaigns_spendAndFlightData = campaignInfo2days
      .join(broadcast(distinct_prevAdjustedCampaigns), "CampaignId")

    // Update pacing data for previously adjusted campaigns
    val prevAdjustedCampaigns_updatedPacingData = updatePrevAdjustedCampaigns(prevAdjustedCampaigns_spendAndFlightData, prevAdjustedCampaigns_campaignAdjustmentsData, date)


    /*** Update all other campaigns ***/

    // Filter current day's spend and flight data for all other campaigns
    val allOtherCampaigns_spendAndFlightData = campaignInfo2days
      .join(broadcast(distinct_prevAdjustedCampaigns), Seq("CampaignId"), "left_anti")
      .filter(col("Date") === lit(date))

    // Identify and label new pc underdelivering campaigns by excluding any previously set test & control campaigns
    val testControlCampaigns_campaignAdjustmentsData = campaignAdjustmentsPacingData
      .filter(
        col("IsTest").isNotNull // Get control and test campaigns
      ).select("CampaignId")

    val newPCUnderdelivering = newPCUnderdeliveringCampaigns.join(testControlCampaigns_campaignAdjustmentsData, Seq("CampaignId"), "left_anti")

    val labelNew_allOtherCampaigns_spendAndFlightData = allOtherCampaigns_spendAndFlightData.alias("ud")
      .join(broadcast(newPCUnderdelivering).alias("new"), col("ud.CampaignId") === col("new.CampaignId"), "left")
      .withColumn("IsNewlyAdded", when(col("new.CampaignId").isNotNull, lit(true)).otherwise(lit(null)))
      .drop(col("new.CampaignId"))

    // Update pacing (to null or 0), flight and adjustment data for all other campaigns
    val allOtherCampaigns_updatedPacingData = updateAllOtherCampaigns(labelNew_allOtherCampaigns_spendAndFlightData, campaignAdjustmentsPacingData, date, testSplit)


    /*** Combine updated pacing data for all campaigns ***/

    val combined_updatedPacingData = prevAdjustedCampaigns_updatedPacingData.union(allOtherCampaigns_updatedPacingData)


    /*** Update CampaignPcAdjustment based on updated pacing status - preselect logic type in job parameter ***/

    val finalCampaignsAdjustments = updateAdjustmentsVersion match {
      case "simple" => updateCampaignAdjustmentSimple(combined_updatedPacingData, date)
      case "complex" => updateCampaignAdjustmentComplex(combined_updatedPacingData, date)
      case _ => throw new IllegalArgumentException("Invalid update campaign adjustments version")
    }

    finalCampaignsAdjustments.selectAs[CampaignAdjustmentsPacingSchema]
  }

  def checkUnderdeliveryUDF(): UserDefinedFunction = udf((spend: Double, fraction: Double) => {
    // Cutoff and scaling factor determined from analyzing results
    val initialCutoffBase = 0.75 // Initial value for cutoff adjustment
    val scalingFactor = 0.25 // Factor for adjusting the cutoff based on underdelivery (smaller factor decreases slower)
    val minThreshold = 0.05 // Minimum threshold for the cutoff

    // Gradually decrease the cutoff as underdelivery increases, using log function
    val baseCutoff = initialCutoffBase - scala.math.log10(scala.math.max(spend, 1)) * scalingFactor
    val adjustedCutoff = scala.math.max(baseCutoff, minThreshold) // Ensure cutoff doesn't go below min threshold

    fraction > adjustedCutoff
  })

  def isUnderdelivering(campaignInfo: DataFrame, date: LocalDate): DataFrame = {
    // Check if campaign is in current flight at least past 3 days
    val checkCurrentDate = date.minusDays(2)
    val currentFlightDaysCt = campaignInfo
      .filter(
        col("IsCurrent") === 1 && col("Date") >= checkCurrentDate
      ).groupBy("CampaignId", "CampaignFlightId")
      .agg(count("*").alias("Days_CurrentFlight"))
      .filter(col("Days_CurrentFlight") === 3)
      .select("CampaignId").distinct()

    // UDF to check underdelivery
    val checkUnderdelivery = checkUnderdeliveryUDF()

    // Apply UDF to check if underdelivering
    val allUnderdeliveringCampaigns = campaignInfo.alias("ud")
      .join(broadcast(currentFlightDaysCt.alias("cur")), col("ud.CampaignId") === col("cur.CampaignId"), "inner")
      .withColumn("AdjustedCutoff", checkUnderdelivery(col("UnderdeliveryInUSD"), col("UnderdeliveryFraction")))
      .filter(
        col("UnderdeliveryInUSD") > 0 &&
          col("AdjustedCutoff")
      ).drop(col("cur.CampaignId"))

    // Check if campaign was underdelivering > 2 in past 5 days
    val underdeliveringDaysCt = allUnderdeliveringCampaigns
      .groupBy("CampaignId", "CampaignFlightId")
      .agg(count("*").alias("Days_Underdelivering"))
      .filter(col("Days_Underdelivering") > 2)

    // Get distinct underdelivering campaigns
    underdeliveringDaysCt.select("CampaignId").distinct()
  }

  def joinCampaignsToPlatformReportAndAggregate(distinct_udcp: DataFrame, platformReportData: Dataset[RtbPlatformReportCondensedData], countryData: Dataset[CountryRecord], adFormatData: Dataset[AdFormatRecord]): DataFrame = {
    // Get rtb data for new underdelivering campaigns
    val rtb = platformReportData.join(broadcast(distinct_udcp), "CampaignId")
      .withColumn("FirstPriceAdjustment", lit(1) - (col("PredictiveClearingSavingsInUSD") / col("BidAmountInUSD")))
      .withColumn("DeviceType", col("DeviceType").cast(IntegerType))
      .withColumn("RenderingContext", col("RenderingContext").cast(IntegerType))

    // Join to country to get region
    val rtbWithRegion = rtb.alias("r")
      .join(broadcast(countryData.alias("c")), col("r.Country") === col("c.LongName"), "left")
      .select(col("r.*"), col("c.ContinentalRegionId"))

    // Get ChannelSimple
    val rtbwithChannel = AddChannel(rtbWithRegion, adFormatData, renderingContextCol = "RenderingContext", deviceTypeCol = "DeviceType")

    // Aggregate metrics by campaign, region & channel
    rtbwithChannel
      .groupBy("CampaignId", "ContinentalRegionId", "ChannelSimple")
      .agg(
        sum(col("BidCount")).as("BidCount"),
        sum(col("ImpressionCount")).as("ImpressionCount"),
        mean(col("FirstPriceAdjustment")).as("Avg_FirstPriceAdjustment")
      )
      .withColumn("WinRate", (col("ImpressionCount") / col("BidCount")))
      .filter(col("Avg_FirstPriceAdjustment").isNotNull) // Filter out campaigns with null pc pushdowns
  }

  def joinToPlatformWideStatsAndCompareRelativeStats(rtb_agg: DataFrame, platformWideStatsData: Dataset[PlatformWideStatsSchema]): DataFrame = {
    // Compare platform-wide stats for region/channel combos with relative stats of the campaigns
    rtb_agg.alias("r")
      .join(broadcast(platformWideStatsData.alias("pw")), col("r.ContinentalRegionId") === col("pw.ContinentalRegionId") && col("r.ChannelSimple") === col("pw.ChannelSimple"), "left")
      .select(
        col("r.CampaignId"),
        col("r.ContinentalRegionId"),
        col("r.ChannelSimple"),
        col("r.WinRate"),
        col("pw.Avg_WinRate").alias("PW_Avg_WinRate"),
        col("pw.Med_WinRate").alias("PW_Med_WinRate"),
        col("r.Avg_FirstPriceAdjustment"),
        col("pw.Avg_FirstPriceAdjustment").alias("PW_Avg_FirstPriceAdjustment"),
        col("pw.Med_FirstPriceAdjustment").alias("PW_Med_FirstPriceAdjustment"),
        col("r.BidCount"),
        col("r.ImpressionCount")
      ).groupBy(
        "CampaignId"
      ).agg(
        collect_list(col("ContinentalRegionId")).alias("ContinentalRegionId"),
        collect_list(col("ChannelSimple")).alias("ChannelSimple"),
        sum(col("BidCount")).alias("BidCount"),
        sum(col("ImpressionCount")).alias("ImpressionCount"),
        mean(col("WinRate")).alias("Avg_WinRate"),
        mean(col("PW_Avg_WinRate")).alias("PW_Avg_WinRate"),
        mean(col("PW_Med_WinRate")).alias("PW_Med_WinRate"),
        mean(col("Avg_FirstPriceAdjustment")).alias("Avg_FirstPriceAdjustment"),
        mean(col("PW_Avg_FirstPriceAdjustment")).alias("PW_Avg_FirstPriceAdjustment"),
        mean(col("PW_Med_FirstPriceAdjustment")).alias("PW_Med_FirstPriceAdjustment")
      ).withColumn(
        "AvgWR_Delta", col("PW_Avg_WinRate") - col("Avg_WinRate")
      ).withColumn(
        "MedWR_Delta", col("PW_Med_WinRate") - col("Avg_WinRate")
      ).withColumn(
        "AvgFPA_Delta", col("PW_Avg_FirstPriceAdjustment") - col("Avg_FirstPriceAdjustment")
      ).withColumn(
        "MedFPA_Delta", col("PW_Med_FirstPriceAdjustment") - col("Avg_FirstPriceAdjustment")
      )
  }

  def filterPlausiblePC(compareStats: DataFrame, checkForAggressiveFPA: Double, checkMinBidCount: Int): DataFrame = {
    /*
     * Consider Win Rate & FPA criteria to determine if plausibily PC:
     * - Criteria of having lower win rate and larger pushdowns compared to relative platform avg/medians (not making these checks too strict)
     * - Filter out low bidding campaigns and DOOH because is PC is bidding at floor 100% of the time
     */
    compareStats.filter(
      (col("AvgWR_Delta") > lit(0) || col("MedWR_Delta") >= lit(-0.1)) &&
        (col("AvgFPA_Delta") > lit(0) || col("MedFPA_Delta") >= lit(-0.1) || col("Avg_FirstPriceAdjustment") <= checkForAggressiveFPA) &&
        (col("BidCount") > checkMinBidCount) &&
        !(size(col("ChannelSimple")) === 1 && array_contains(col("ChannelSimple"), "Digital Out Of Home"))
    )
  }

  def checkDiscrepancyPC(pcResultsMergedData: Dataset[PcResultsMergedDataset], platWide_underdeliveringCampaigns: DataFrame): DataFrame = {
    // Compare with discrepancy to ensure it exceeds the average PC pushdown
    pcResultsMergedData.join(broadcast(platWide_underdeliveringCampaigns), "CampaignId")
      .select("CampaignId", "Discrepancy", "BidsFirstPriceAdjustment")//, "IsValuePacing")
      .withColumn("DiscrepancyAdjustment", lit(1) / col("Discrepancy"))
      .groupBy("CampaignId")//, "IsValuePacing")
      .agg(
        mean(col("DiscrepancyAdjustment")).alias("BR_Avg_DiscrepancyAdjustment"),
        mean(col("BidsFirstPriceAdjustment")).alias("BR_Avg_FirstPriceAdjustment")
      ).filter(
        col("BR_Avg_DiscrepancyAdjustment") - col("BR_Avg_FirstPriceAdjustment") > lit(0.1)
      ).repartition(numPartitions, col("CampaignId"))
  }

  def updatePrevAdjustedCampaigns(prevAdjustedCampaigns_spendAndFlightData: DataFrame, prevAdjustedCampaigns_campaignAdjustmentsData: Dataset[CampaignAdjustmentsPacingSchema], date: LocalDate): DataFrame = {
    // Update flight details, pacing status, and spend performance of previously adjusted campaigns

    // UDF to calculate percent difference
    val calcPctDeltaUDF = udf((oldValue: Double, newValue: Double) => {
      val adjustedOldValue = if (oldValue == 0) valCloseTo0 else oldValue
      ((newValue - adjustedOldValue) / adjustedOldValue) * 100
    })

    // Determine new pacing status based on percentage change in UnderdeliveryFraction
    val windowSpec = Window.partitionBy("CampaignId").orderBy("Date")
    val getPacingStatus = prevAdjustedCampaigns_spendAndFlightData
      .withColumn(
        "prevday_UnderdeliveryFraction",
        lag("UnderdeliveryFraction", 1).over(windowSpec)
      ).withColumn(
        "PctDelta_UnderdeliveryFraction",
        calcPctDeltaUDF(col("prevday_UnderdeliveryFraction"), col("UnderdeliveryFraction"))
      ).filter(
        col("Date") === lit(date)
      ).withColumn(
        "PacingStatus",
        when(col("UnderdeliveryFraction") < maxPacingUnderdeliveryFraction, "Pacing")
          .when(col("UnderdeliveryFraction") >= maxPacingUnderdeliveryFraction,
            when((col("PctDelta_UnderdeliveryFraction") >= improvedPacingThresholdPct && col("PctDelta_UnderdeliveryFraction") <= 0) || col("PctDelta_UnderdeliveryFraction").isNull, "MinorImprovedNotPacing")
              .when(col("PctDelta_UnderdeliveryFraction") < improvedPacingThresholdPct, "MajorImprovedNotPacing")
              .when(col("PctDelta_UnderdeliveryFraction") > 0, "WorseNotPacing")
          )
      ).select(
        "CampaignId", "CampaignFlightId", "EndDateExclusiveUTC", "PacingStatus", "EstimatedBudgetInUSD", "TotalAdvertiserCostFromPerformanceReportInUSD", "UnderdeliveryFraction", "UnderdeliveryInUSD", "OverdeliveryInUSD", "MinCalculatedCampaignCapInUSD", "MaxCalculatedCampaignCapInUSD"
      )

    // Update previously adjusted campaigns with new pacing, flight and spend data
    val prevAdjustedCampaigns_updatedPacingData = prevAdjustedCampaigns_campaignAdjustmentsData.alias("adj")
      .join(getPacingStatus.alias("cf"), Seq("CampaignId"))
      .withColumn("CampaignPCAdjustment",
        when(col("adj.CampaignFlightId") =!= col("cf.CampaignFlightId"), lit(defaultAdjustment)) // Revert adjustment to default if new flight
          .otherwise(col("CampaignPCAdjustment"))
      ).withColumn(
        "AddedDate",
        when(col("adj.CampaignFlightId") =!= col("cf.CampaignFlightId"), null) // Revert AddedDate to null if new flight
          .otherwise(col("AddedDate"))
      )
      // Update pacing counters
      .withColumn("Prev_Pacing", col("Pacing"))
      .withColumn("Prev_ImprovedNotPacing", col("ImprovedNotPacing"))
      .withColumn("Prev_WorseNotPacing", col("WorseNotPacing"))
      .withColumn(
        "Pacing",
        when(col("Prev_Pacing") > 0 && col("PacingStatus") === "Pacing", col("Prev_Pacing") + 1) // Pacing before and still pacing, add 1
          .when(col("Prev_Pacing") === 0 && col("PacingStatus") === "Pacing", 1) // Wasn't pacing before, now pacing - change to 1
          .when(col("Prev_Pacing") > 0 && col("PacingStatus") =!= "Pacing", 0) // Was pacing before, not pacing now - revert to 0
          .otherwise(col("Prev_Pacing"))
      ).withColumn(
        "ImprovedNotPacing",
        when(col("Prev_ImprovedNotPacing") > 0 && col("PacingStatus").isin("MinorImprovedNotPacing", "MajorImprovedNotPacing"), col("Prev_ImprovedNotPacing") + 1) // Improved not pacing before and still improving but not pacing, add 1
          .when(col("Prev_ImprovedNotPacing") === 0 && col("PacingStatus").isin("MinorImprovedNotPacing", "MajorImprovedNotPacing"), 1) // Was not before, but improving not pacing now - change to 1
          .when(col("Prev_ImprovedNotPacing") > 0 && !col("PacingStatus").isin("MinorImprovedNotPacing", "MajorImprovedNotPacing"), 0) // Improving not pacing before, now not - revert to 0
          .otherwise(col("Prev_ImprovedNotPacing"))
      ).withColumn(
        "WorseNotPacing",
        when(col("Prev_WorseNotPacing") > 0 && col("PacingStatus") === "WorseNotPacing", col("Prev_WorseNotPacing") + 1) // Worse not pacing before and still worse not pacing, add 1
          .when(col("Prev_WorseNotPacing") === 0 && col("PacingStatus") === "WorseNotPacing", 1) // Was not before, worse not pacing now - change to 1
          .when(col("Prev_WorseNotPacing") > 0 && col("PacingStatus") =!= "WorseNotPacing", 0) // Worse not pacing before, now pacing better - revert to 0
          .otherwise(col("Prev_WorseNotPacing"))
      ).withColumn(
        "temp_MinorImprovedNotPacing",
        when(col("PacingStatus") === "MinorImprovedNotPacing", 1).otherwise(0)
      ).withColumn(
        "temp_MajorImprovedNotPacing",
        when(col("PacingStatus") === "MajorImprovedNotPacing", 1).otherwise(0)
      ).select(
        // Update campaign details with current day's data
        "adj.CampaignId", "cf.CampaignFLightId", "CampaignPCAdjustment", "adj.IsTest", "AddedDate", "cf.EndDateExclusiveUTC", "adj.IsValuePacing",
        "Pacing", "ImprovedNotPacing", "WorseNotPacing", "Prev_Pacing", "Prev_ImprovedNotPacing", "Prev_WorseNotPacing", "temp_MinorImprovedNotPacing", "temp_MajorImprovedNotPacing",
        "cf.EstimatedBudgetInUSD", "cf.TotalAdvertiserCostFromPerformanceReportInUSD", "cf.UnderdeliveryFraction", "cf.UnderdeliveryInUSD", "cf.OverdeliveryInUSD", "cf.MinCalculatedCampaignCapInUSD", "cf.MaxCalculatedCampaignCapInUSD"
      )

    prevAdjustedCampaigns_updatedPacingData
  }

  def updateAllOtherCampaigns(labelNew_allOtherCampaigns_spendAndFlightData: DataFrame, campaignAdjustmentsPacingData: Dataset[CampaignAdjustmentsPacingSchema], date: LocalDate, testSplit: Option[Double]): DataFrame = {
    // Update flight details, pacing status, spend performance, and CampaignPcAdjustment of all other campaigns
    // These include newly added pc underdelivering, previously reset, control (if test split), and not-pc-underdelivering campaigns

    // Set IsTest for new pc underdelivering campaigns based on hash function
    val assignTestGroupUDF = udf((CampaignId: String) => assignTestGroup(CampaignId, testSplit))

    /*
     * For new pc underdelivering campaigns:
     *   - Set new CampaignPcAdjustment (at last step), assign Test group, update AddedDate, update IsValuePacing, set pacing status to 0
     *   - Update campaign flight and spend with current day's data
     * For rest (non-adjusted campaigns):
     *   - Keep CampaignPcAdjustment as default, do not assign Test group, and keep AddedDate and pacing status as null
     *   - Update campaign flight and spend with current day's data
     */
    val labelNew_allOtherCampaigns_updatedPacingData = campaignAdjustmentsPacingData.alias("adj")
      .drop(col("adj.IsValuePacing"))
      .join(labelNew_allOtherCampaigns_spendAndFlightData.alias("ud"), col("ud.CampaignId") === col("adj.CampaignId"), "right")
      .withColumn("CampaignPcAdjustment", lit(defaultAdjustment) // Set to default
      ).withColumn("IsTest",
        when(col("IsNewlyAdded"), assignTestGroupUDF(col("ud.CampaignId"))) // Assign test group to new campaigns
          .otherwise(col("IsTest")) // Keep previous setting or null if never hit pc underdelivery logic
      ).withColumn("AddedDate",
        when(col("IsNewlyAdded"), date) // Set AddedDate for new campaigns
          .otherwise(null) // Keep or reset to null
      ).withColumn("IsValuePacing",
        col("ud.IsValuePacing") // Use most recently pulled IsValuePacing status
      ).withColumn("Pacing",
        when(col("IsNewlyAdded"), lit(0))
          .otherwise(lit(null).cast(IntegerType))
      ).withColumn("ImprovedNotPacing",
        when(col("IsNewlyAdded"), lit(0))
          .otherwise(lit(null).cast(IntegerType))
      ).withColumn("WorseNotPacing",
        when(col("IsNewlyAdded"), lit(0))
          .otherwise(lit(null).cast(IntegerType))
      ).withColumn("Prev_Pacing", lit(null).cast(IntegerType))
      .withColumn("Prev_ImprovedNotPacing", lit(null).cast(IntegerType))
      .withColumn("Prev_WorseNotPacing", lit(null).cast(IntegerType))
      .withColumn("temp_MinorImprovedNotPacing", lit(0).cast(IntegerType))
      .withColumn("temp_MajorImprovedNotPacing", lit(0).cast(IntegerType))
      .select(
        "IsNewlyAdded", "ud.CampaignId", "ud.CampaignFlightId", "CampaignPCAdjustment", "IsTest", "AddedDate", "ud.EndDateExclusiveUTC", "IsValuePacing",
        "Pacing", "ImprovedNotPacing", "WorseNotPacing", "Prev_Pacing", "Prev_ImprovedNotPacing", "Prev_WorseNotPacing", "temp_MinorImprovedNotPacing", "temp_MajorImprovedNotPacing",
        "ud.EstimatedBudgetInUSD", "ud.TotalAdvertiserCostFromPerformanceReportInUSD", "ud.UnderdeliveryFraction", "ud.UnderdeliveryInUSD", "ud.OverdeliveryInUSD", "ud.MinCalculatedCampaignCapInUSD", "ud.MaxCalculatedCampaignCapInUSD"
      )

    // Set new CampaignPcAdjustment for new pc underdelivering campaigns in Test group
    val allOtherCampaigns_updatedPacingData = labelNew_allOtherCampaigns_updatedPacingData
      .withColumn("CampaignPcAdjustment",
        when(col("IsTest") && col("IsNewlyAdded"), defaultAdjustment - stepSizeAdjustment) // Set new CampaignPCAdjustment for new campaigns
          .otherwise(col("CampaignPcAdjustment")) // Keep previous default
      ).drop("IsNewlyAdded")

    allOtherCampaigns_updatedPacingData
  }

  def updateCampaignAdjustmentSimple(updateFlightSettings_CampaignsAdjustments: DataFrame, date: LocalDate): DataFrame = {
    val nextDayDate = lit(date.plusDays(1).toString)
    val nextDayPlusTwoDays = lit(date.plusDays(3).toString)

    /*
      Update "CampaignPCAdjustment" based on pacing performance:
      1. New Flight: Keep adjustment set to default.
      2. Pacing: Keep adjustment the same.
      3. Not Pacing but Improving: Keep adjustment the same.
      4. Not Pacing and Not Improving:
          a. Ending Soon: Lower adjustment by aggressive step size if ending soon.
          b. Not Ending Soon: Lower adjustment by regular step size.
          c. Over 3 Days: Revert to default adjustment if not improving for over 3 days.
      5. Cap adjustment within min and max thresholds.
    */
    updateFlightSettings_CampaignsAdjustments
      .withColumn(
        "CampaignPCAdjustment",
        when(col("CampaignPCAdjustment") === defaultAdjustment, col("CampaignPCAdjustment")) // New flight
          .when(col("Pacing") > 0, col("CampaignPCAdjustment")) // Pacing
          .when(col("ImprovedNotPacing") > 0, col("CampaignPCAdjustment")) // Not pacing but improving
          .when(col("WorseNotPacing") > 0 // Not pacing and not improving, ending soon
            && col("WorseNotPacing") <= 3
            && col("EndDateExclusiveUTC").cast("date").between(nextDayDate, nextDayPlusTwoDays),
            col("CampaignPCAdjustment") - aggressiveStepSizeAdjustment)
          .when(col("WorseNotPacing") > 0 // Not pacing and not improving
            && col("WorseNotPacing") <= 3,
            col("CampaignPCAdjustment") - stepSizeAdjustment)
          .when(col("WorseNotPacing") > 3, defaultAdjustment) // Not pacing and not improving over 3 days
          .otherwise(col("CampaignPCAdjustment"))
      ).withColumn(
        "CampaignPCAdjustment", // Set min and max threshold
        when(col("CampaignPCAdjustment") > defaultAdjustment, defaultAdjustment) // Cap at 100% adjustment
          .when(col("CampaignPCAdjustment") <= minAdjustment, minAdjustment) // Cap at min
          .otherwise(col("CampaignPCAdjustment"))
      )
  }

  def updateCampaignAdjustmentComplex(updateFlightSettings_CampaignsAdjustments: DataFrame, date: LocalDate): DataFrame = {
    val previousDayDate = lit(date.minusDays(1).toString)
    val nextDayDate = lit(date.plusDays(1).toString)
    val nextDayPlusTwoDays = lit(date.plusDays(3).toString)

    /*
      Update "CampaignPCAdjustment" based on pacing performance:
      1. New Flight: Keep adjustment set to default.
      2. Pacing: Keep adjustment the same.
      3. Not Pacing but Improving:
          a. Major improvement: Keep adjustment the same.
          b. Minor improvement, Ending Soon: Lower adjustment by aggressive step size if ending soon.
          c. Minor improvement, Not Ending Soon: Lower adjustment by regular step size.
      4. Not Pacing and Not Improving:
          a. After Previously Pacing or Improving: Revert the adjustment by a regular step size.
          b. Ending Soon: Lower adjustment by aggressive step size if ending soon.
          c. Not Ending Soon: Lower adjustment by regular step size.
          d. Over 3 Days: Revert to default adjustment if not improving for over 3 days.
      5. Cap adjustment within min and max thresholds.
    */
    updateFlightSettings_CampaignsAdjustments
      .withColumn(
        "CampaignPCAdjustment",
        when(col("CampaignPCAdjustment") === defaultAdjustment, col("CampaignPCAdjustment")) // New flight
          .when(col("Pacing") > 0, col("CampaignPCAdjustment")) // Pacing
          .when(col("ImprovedNotPacing") > 0
            && col("temp_MajorImprovedNotPacing") === 1,
            col("CampaignPCAdjustment")) // Major improvement
          .when(col("ImprovedNotPacing") > 0
            && col("temp_MinorImprovedNotPacing") === 1
            && col("EndDateExclusiveUTC").cast("date").between(nextDayDate, nextDayPlusTwoDays),
            col("CampaignPCAdjustment") - aggressiveStepSizeAdjustment) // Minor improvement, ending soon
          .when(col("ImprovedNotPacing") > 0
            && col("temp_MinorImprovedNotPacing") === 1,
            col("CampaignPCAdjustment") - stepSizeAdjustment) // Minor improvement, not ending soon
          .when(col("WorseNotPacing") === 1
            && col("AddedDate") < previousDayDate,
            col("CampaignPCAdjustment") + stepSizeAdjustment) // Not pacing after previously pacing or improving
          .when(col("WorseNotPacing") > 0
            && col("WorseNotPacing") <= 3
            && col("EndDateExclusiveUTC").cast("date").between(nextDayDate, nextDayPlusTwoDays),
            col("CampaignPCAdjustment") - aggressiveStepSizeAdjustment) // Not pacing and not improving, ending soon
          .when(col("WorseNotPacing") > 0
            && col("WorseNotPacing") <= 3,
            col("CampaignPCAdjustment") - stepSizeAdjustment) // Not pacing and not improving, not ending soon
          .when(col("WorseNotPacing") > 3, defaultAdjustment) // Not pacing and not improving over 3 days
          .otherwise(col("CampaignPCAdjustment"))
      ).withColumn(
        "CampaignPCAdjustment", // Set min and max threshold
        when(col("CampaignPCAdjustment") > defaultAdjustment, defaultAdjustment) // Cap at 100% adjustment
          .when(col("CampaignPCAdjustment") <= minAdjustment, minAdjustment) // Cap at min
          .otherwise(col("CampaignPCAdjustment"))
      )
  }

  def assignTestGroup(entityId: String, thresholdPct: Option[Double]): Boolean = {
    import scala.util.hashing.MurmurHash3
    val hash = Math.abs(MurmurHash3.stringHash(entityId))

    // Use modulus to split into groups
    val threshold = thresholdPct match {
      case Some(rate) if rate >= 0 && rate <= 1 => (rate * Int.MaxValue).toInt
      case Some(_) => throw new IllegalArgumentException("Invalid split threshold, should be between 0 and 1")
      case None => Int.MaxValue // If None, apply backoff to all new campaigns
    }

    (Math.abs(hash) < threshold)
  }

  def transform(date: LocalDate, testSplit: Option[Double], updateAdjustmentsVersion: String, fileCount: Int): Unit = {

    // This dataset contains multiple entries with different dates - 5 day look-back.
    val campaignUnderdeliveryData = loadParquetDataDailyV2[CampaignThrottleMetricSchema](
      CampaignThrottleMetricDataset.S3PATH,
      CampaignThrottleMetricDataset.S3PATH_DATE_GEN,
      date,
      4,
      nullIfColAbsent = false // Setting this to false since nullIfColAbsent sets date to null (its a bug with selectAs)
    ).withColumn(
      "Date",
      to_date(col("Date"))
    ).selectAs[CampaignThrottleMetricSchema]

    val campaignAdjustmentsPacingData = try {
      println("Attempting to read CampaignAdjustmentsDataset...")
      val data = loadParquetDataDailyV2[CampaignAdjustmentsPacingSchema](
        CampaignAdjustmentsPacingDataset.S3_PATH(envForWrite),
        CampaignAdjustmentsPacingDataset.S3_PATH_DATE_GEN,
        date.minusDays(1), // get previous day's adjustment data
        nullIfColAbsent = false // Setting this to false since we want this query to fail if data doesn't exist
      )
      println("Previous day's CampaignAdjustmentsPacingDataset exists and loaded successfully.")
      data
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        println("Previous day's CampaignAdjustmentsPacingDataset does not exist, so returning empty dataset.")
        spark.emptyDataset[CampaignAdjustmentsPacingSchema]
      case unknown: Exception =>
        println(s"Error occurred: ${unknown.getMessage}.")
        throw unknown
    }

    val campaignFlightData = loadParquetCampaignFlightLatestPartitionUpTo(
      CampaignFlightDataset.S3PATH,
      CampaignFlightDataset.S3PATH_DATE_GEN,
      date
    )

    val platformWideStatsData = loadParquetDataDailyV2[PlatformWideStatsSchema](
      PlatformWideStatsDataset.S3_PATH(envForWrite),
      PlatformWideStatsDataset.S3_PATH_DATE_GEN,
      date
    )

    val pcResultsMergedData = loadParquetDataDailyV2[PcResultsMergedDataset](
      PcResultsMergedDataset.S3_PATH(Some(envForRead)),
      PcResultsMergedDataset.S3_PATH_DATE_GEN,
      date,
      nullIfColAbsent = true
    )

    val platformReportData = UnifiedRtbPlatformReportDataSet.readRange(date, date.plusDays(1))
      .selectAs[RtbPlatformReportCondensedData]

    val countryData = CountryDataSet().readLatestPartitionUpTo(date)
    val adFormatData = AdFormatDataSet().readLatestPartitionUpTo(date)


    val (campaignInfo, underdeliveringCampaigns) = getUnderdelivery(campaignUnderdeliveryData, campaignFlightData, date)

    val pc_underdeliveringCampaigns = getUnderdeliveringDueToPlutus(underdeliveringCampaigns, platformReportData, countryData, adFormatData, platformWideStatsData, pcResultsMergedData)

    val finalCampaignAdjustments = updateAdjustments(campaignInfo, date, pc_underdeliveringCampaigns, campaignAdjustmentsPacingData, testSplit, updateAdjustmentsVersion)

    finalCampaignAdjustments.cache().count()

    // Counts to monitor
    val newCampaignsCount = finalCampaignAdjustments.filter(col("AddedDate") === date && col("Pacing") === 0 && col("ImprovedNotPacing") === 0 && col("WorseNotPacing") === 0).count()
    val endedFlightResetCampaignsCount = finalCampaignAdjustments.filter(col("AddedDate").isNull && col("CampaignPCAdjustment") === 1 && (col("Pacing") > 0 || col("ImprovedNotPacing") > 0 || col("WorseNotPacing") > 0)).count()
    val worsePacingResetCampaignsCount = finalCampaignAdjustments.filter(col("WorseNotPacing") > 3 && col("CampaignPCAdjustment") === 1).count()
    val activeAdjustedCampaignsCount = finalCampaignAdjustments.filter(col("AddedDate").isNotNull && col("CampaignPCAdjustment") =!= 1).count()

    val isValuePacingCount = finalCampaignAdjustments.filter(col("IsValuePacing") === true && col("AddedDate").isNotNull && col("CampaignPCAdjustment") =!= 1).count()
    val isValuePacingRatioOfAdjustedCampaigns = isValuePacingCount.toDouble / activeAdjustedCampaignsCount.toDouble

    val controlGroupCount = finalCampaignAdjustments.filter(col("IsTest") === false).count()
    val testGroupCount = finalCampaignAdjustments.filter(col("IsTest") === true).count()
    val totalSplitCount = controlGroupCount + testGroupCount
    val controlGroupRatio = controlGroupCount.toDouble / totalSplitCount.toDouble
    val testGroupRatio = testGroupCount.toDouble / totalSplitCount.toDouble

    val listener = new WriteListener()
    spark.sparkContext.addSparkListener(listener)

    val campaignAdjustmentsPacing_outputPath = CampaignAdjustmentsPacingDataset.S3_PATH_DATE(date, envForWrite)
    finalCampaignAdjustments.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(campaignAdjustmentsPacing_outputPath)

    val onlyCampaignAdjustments = finalCampaignAdjustments.filter(col("CampaignPCAdjustment") =!= 1).select("CampaignId", "CampaignPCAdjustment")

    val campaignAdjustments_outputPath = CampaignAdjustmentsDataset.S3_PATH_DATE(date, envForWrite)
    onlyCampaignAdjustments.coalesce(fileCount)
      .write.mode(SaveMode.Overwrite)
      .parquet(campaignAdjustments_outputPath)

    println(s"CampaignAdjustmentsPacingOutputPath: $campaignAdjustmentsPacing_outputPath")
    println(s"CampaignAdjustmentsOutputPath: $campaignAdjustments_outputPath")

    val rows = listener.rowsWritten
    println(s"Rows Written: $rows")

    numRowsWritten.set(rows)
    campaignCounts.labels("NewCampaigns").set(newCampaignsCount)
    campaignCounts.labels("EndedFlightCampaigns").set(endedFlightResetCampaignsCount)
    campaignCounts.labels("WorsePacingCampaigns").set(worsePacingResetCampaignsCount)
    campaignCounts.labels("ActiveAdjustedCampaigns").set(activeAdjustedCampaignsCount)
    campaignCounts.labels("DACampaigns").set(isValuePacingRatioOfAdjustedCampaigns)
    testControlSplit.labels("TotalTestControlCampaigns").set(totalSplitCount)
    testControlSplit.labels("ControlCampaigns").set(controlGroupRatio)
    testControlSplit.labels("TestCampaigns").set(testGroupRatio)

  }
}