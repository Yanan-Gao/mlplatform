package com.thetradedesk.frequency.transform

import java.time.LocalDate

import com.thetradedesk.logging.Logger
import com.thetradedesk.frequency.FrequencyDailyAggregationConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FrequencyDailyAggregationTransform extends Logger {

  def buildRoiAdGroupKeys(
      spark: SparkSession,
      runDate: LocalDate,
      roiGoalTypes: Seq[Int]
  ): DataFrame = {
    import com.thetradedesk.frequency.schema.{UnifiedAdGroupDataSet, CampaignROIGoalDataSet}
    val adLatest = UnifiedAdGroupDataSet().readLatestPartitionUpTo(runDate, isInclusive = true)
    val croi = CampaignROIGoalDataSet().readLatestPartitionUpTo(runDate, isInclusive = true)
      .where(col("Priority") === 1)

    val joined = adLatest.alias("ag")
      .join(croi.alias("cg"), Seq("CampaignId"), "left")
      .withColumn(
        "goal",
        coalesce(col("cg.ROIGoalTypeId").cast("int"), col("ag.ROIGoalTypeId").cast("int"))
      )

    val goalSet = roiGoalTypes.toSet
    require(goalSet.nonEmpty, "roiGoalTypes must contain at least one entry")

    joined
      .where(col("goal").isin(goalSet.toSeq.map(_.asInstanceOf[AnyRef]): _*))
      .select(
        col("ag.AdGroupId").alias("ROI_AdGroupId"),
        col("ag.CampaignId").alias("ROI_CampaignId")
      )
      .dropDuplicates("ROI_AdGroupId", "ROI_CampaignId")
  }

  def transform(
      bidsImpressions: DataFrame,
      clicks: DataFrame,
      roiKeys: DataFrame
  ): (DataFrame, DataFrame) = {
    val filteredImpressions = bidsImpressions
      .select(
        "IsImp",
        "BidRequestId",
        "UIID",
        "AdGroupId",
        "CampaignId",
        "AdvertiserId",
        "LogEntryTime"
      )
      .where(col("IsImp") === true)

    val impressionsWithRoi = filteredImpressions
      .join(broadcast(roiKeys),
        col("AdGroupId") === col("ROI_AdGroupId") &&
          col("CampaignId") === col("ROI_CampaignId"),
        "inner"
      )
      .drop("ROI_AdGroupId", "ROI_CampaignId")
      .where(
        col("UIID").isNotNull &&
          col("UIID") =!= "00000000-0000-0000-0000-00000" &&
          col("UIID") =!= "00000000-0000-0000-0000-000000000000"
      )

    val dedupedClicks = clicks
      .select("BidRequestId")
      .dropDuplicates("BidRequestId")

    val enriched = impressionsWithRoi
      .join(dedupedClicks.withColumn("has_click", lit(1)), Seq("BidRequestId"), "left")
      .withColumn("has_click", coalesce(col("has_click"), lit(0)))
      .withColumn("date", to_date(col("LogEntryTime")))
      .withColumn("impression_count", lit(1L))
      .withColumn("click_count", col("has_click").cast("long"))

    val aggregates = enriched
      .groupBy("UIID", "CampaignId", "AdvertiserId", "date")
      .agg(
        sum("impression_count").alias("impression_count"),
        sum("click_count").alias("click_count")
      )

    val (filteredAggregates, clickBotUiids) = filterClickBots(aggregates)
    (filteredAggregates, clickBotUiids)
  }

  private def filterClickBots(aggregates: DataFrame): (DataFrame, DataFrame) = {
    val highCampaignDailyBots = aggregates
      .where(col("impression_count") > 500)
      .select("UIID")
      .dropDuplicates("UIID")

    val highDailyBots = aggregates
      .groupBy("UIID", "date")
      .agg(sum("impression_count").alias("daily_impression_count"))
      .where(col("daily_impression_count") > 3000)
      .select("UIID")
      .dropDuplicates("UIID")

    val aggregatedByUiid = aggregates
      .groupBy("UIID")
      .agg(
        sum("impression_count").alias("uiid_impression_count"),
        sum("click_count").alias("uiid_click_count")
      )
      .withColumn(
        "ctr",
        when(col("uiid_impression_count") > 0,
          col("uiid_click_count") / col("uiid_impression_count")
        ).otherwise(lit(0.0))
      )
      .where(col("uiid_impression_count") >= FrequencyDailyAggregationConstants.Impression97)

    val sortedControlPoints = FrequencyDailyAggregationConstants.ClickBotControlPoints.sortBy(_._1)
    val composedCondition = sortedControlPoints.indices.foldLeft(lit(false): org.apache.spark.sql.Column) {
      case (acc, idx) =>
        val (lower, threshold) = sortedControlPoints(idx)
        val condition = if (idx < sortedControlPoints.length - 1) {
          val (upper, _) = sortedControlPoints(idx + 1)
          col("uiid_impression_count") >= lower &&
            col("uiid_impression_count") < upper &&
            col("ctr") >= threshold
        } else {
          col("uiid_impression_count") >= lower && col("ctr") >= threshold
        }
        acc || condition
    }
    val clickBotUiids = aggregatedByUiid
      .where(composedCondition)
      .select("UIID")
      .union(highCampaignDailyBots)
      .union(highDailyBots)
      .dropDuplicates("UIID")

    val filteredAggregates = aggregates.join(clickBotUiids, Seq("UIID"), "left_anti")
    (filteredAggregates, clickBotUiids)
  }

  private def yyyymmdd(date: LocalDate): String = f"${date.getYear}%04d${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
}
