package com.thetradedesk.frequency.transform

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object ShortWindowFrequencyTransform {

  def buildRoiAdGroupKeys(
      spark: SparkSession,
      runDate: LocalDate,
      roiGoalTypes: Seq[Int]
  ): DataFrame = {
    import com.thetradedesk.frequency.schema.{UnifiedAdGroupDataSet, CampaignROIGoalDataSet}
    val latest = UnifiedAdGroupDataSet().readLatestPartitionUpTo(runDate, isInclusive = true)
    val campaignRoi = CampaignROIGoalDataSet().readLatestPartitionUpTo(runDate, isInclusive = true)
      .where(col("Priority") === 1)

    val goalSet = roiGoalTypes.toSet
    require(goalSet.nonEmpty, "roiGoalTypes must contain at least one value")

    latest.alias("ag")
      .join(campaignRoi.alias("cg"), Seq("CampaignId"), "left")
      .withColumn(
        "goal",
        coalesce(col("cg.ROIGoalTypeId").cast("int"), col("ag.ROIGoalTypeId").cast("int"))
      )
      .where(col("goal").isin(goalSet.toSeq.map(_.asInstanceOf[AnyRef]): _*))
      .select(
        col("ag.AdGroupId").alias("ROI_AdGroupId"),
        col("ag.CampaignId").alias("ROI_CampaignId")
      )
      .dropDuplicates("ROI_AdGroupId", "ROI_CampaignId")
  }

  def prepareBase(
      bidsToday: DataFrame,
      bidsPrev: DataFrame,
      clicksToday: DataFrame,
      clicksPrev: DataFrame,
      roiKeys: DataFrame,
      uiidBotFiltering: Option[DataFrame],
      isolatedAdvertisers: Option[DataFrame]
  ): DataFrame = {
    val impressions = bidsToday
      .unionByName(bidsPrev, allowMissingColumns = true)
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

    val joinedRoi = impressions
      .join(
        broadcast(roiKeys),
        col("AdGroupId") === col("ROI_AdGroupId") && col("CampaignId") === col("ROI_CampaignId"),
        "inner"
      )
      .drop("ROI_AdGroupId", "ROI_CampaignId")
      .where(
        col("UIID").isNotNull &&
          col("UIID") =!= "00000000-0000-0000-0000-00000" &&
          col("UIID") =!= "00000000-0000-0000-0000-000000000000"
      )

    val filteredBots = uiidBotFiltering match {
      case Some(botDf) => joinedRoi.join(broadcast(botDf), Seq("UIID"), "left_anti")
      case None => joinedRoi
    }

    val filteredAdvertisers = isolatedAdvertisers match {
      case Some(isoDf) => filteredBots.join(broadcast(isoDf), Seq("AdvertiserId"), "left_anti")
      case None => filteredBots
    }

    val clicksUnion = clicksToday.unionByName(clicksPrev, allowMissingColumns = true)
    val dedupClicks = clicksUnion.select("BidRequestId").dropDuplicates("BidRequestId")

    filteredAdvertisers
      .join(dedupClicks.withColumn("has_click", lit(1)), Seq("BidRequestId"), "left")
      .withColumn("has_click", coalesce(col("has_click"), lit(0)))
      .withColumn("impression_time_unix", col("LogEntryTime").cast("long"))
      .withColumn("date", to_date(from_unixtime(col("impression_time_unix"))))
      .withColumn("label", col("has_click").cast("int"))
      .select("BidRequestId", "UIID", "CampaignId", "AdvertiserId", "impression_time_unix", "date", "label")
  }

  def calculateShortWindowFeatures(
      base: DataFrame,
      runDate: LocalDate,
      windowSizes: Seq[Int],
      blackout: Int
  ): DataFrame = {
    val cached = base.persist(StorageLevel.MEMORY_AND_DISK)
    cached.count()

    val sameCampaign = calcSameCampaignShort(cached, windowSizes, blackout)
    val sameCampaignToday = sameCampaign.where(col("date") === lit(java.sql.Date.valueOf(runDate)))
    val crossCampaign = calcCrossCampaignShort(cached, windowSizes, blackout)
    val sameDay = calcSameDayCounts(sameCampaignToday, blackout)
    val joined = sameDay.join(crossCampaign, Seq("BidRequestId"), "left")

    val featureCols = joined.columns.filter(c => c.startsWith("campaign_") || c.startsWith("user_"))

    val result = joined
      .where(col("date") === lit(java.sql.Date.valueOf(runDate)))
      .select(
        Seq(
          col("BidRequestId"),
          col("UIID"),
          col("CampaignId"),
          col("AdvertiserId"),
          col("impression_time_unix"),
          col("date"),
          col("label")
        ) ++ featureCols.map(col): _*
      )

    cached.unpersist()
    result
  }

  def calcSameCampaignShort(df: DataFrame, windowSizes: Seq[Int], blackout: Int): DataFrame = {
    val colsToAppend = windowSizes.map(getWindowName).zip(windowSizes).flatMap { case (name, windowSize) =>
      val win = Window
        .partitionBy("UIID", "CampaignId")
        .orderBy("impression_time_unix")
        .rangeBetween(-windowSize, blackout)
      Seq(
        count(lit(1)).over(win).alias(s"campaign_impression_count_$name"),
        sum(col("label")).over(win).alias(s"campaign_click_count_$name")
      )
    }
    df.select(df.columns.map(col) ++ colsToAppend: _*)
  }

  def calcCrossCampaignShort(df: DataFrame, windowSizes: Seq[Int], blackout: Int): DataFrame = {
    val colsToAppend = windowSizes.map(getWindowName).zip(windowSizes).flatMap { case (name, windowSize) =>
      val win = Window
        .partitionBy("UIID")
        .orderBy("impression_time_unix")
        .rangeBetween(-windowSize, blackout)
      Seq(
        count(lit(1)).over(win).alias(s"user_impression_count_$name"),
        sum(col("label")).over(win).alias(s"user_click_count_$name")
      )
    }
    df.select((Seq(col("BidRequestId")) ++ colsToAppend): _*)
  }

  def calcSameDayCounts(df: DataFrame, blackout: Int): DataFrame = {
    val campaignWin = Window
      .partitionBy("UIID", "CampaignId")
      .orderBy("impression_time_unix")
      .rangeBetween(Window.unboundedPreceding, blackout)

    val userWin = Window
      .partitionBy("UIID")
      .orderBy("impression_time_unix")
      .rangeBetween(Window.unboundedPreceding, blackout)

    df.select(
      df.columns.map(col) ++ Seq(
        count(lit(1)).over(campaignWin).alias("campaign_impression_count_same_day"),
        sum(col("label")).over(campaignWin).alias("campaign_click_count_same_day"),
        count(lit(1)).over(userWin).alias("user_impression_count_same_day"),
        sum(col("label")).over(userWin).alias("user_click_count_same_day")
      ): _*
    )
  }

  private def getWindowName(seconds: Int): String = {
    if (seconds < 3600) s"${seconds / 60}min"
    else if (seconds < 86400) s"${seconds / 3600}hr"
    else s"${seconds / 86400}day"
  }

  private def yyyymmdd(date: LocalDate): String =
    f"${date.getYear}%04d${date.getMonthValue}%02d${date.getDayOfMonth}%02d"
}
