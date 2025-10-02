package com.thetradedesk.frequency.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RecencyFeaturesTransform {

  def prepareBase(
      bidsImpressions: DataFrame,
      clicks: DataFrame,
      roiKeys: DataFrame,
      uiidBotFiltering: Option[DataFrame],
      isolatedAdvertisers: Option[DataFrame]
  ): DataFrame = {
    val bi = bidsImpressions
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

    val biRoi = bi
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

    val afterBots = uiidBotFiltering match {
      case Some(bot) => biRoi.join(broadcast(bot), Seq("UIID"), "left_anti")
      case None => biRoi
    }

    val afterIsolated = isolatedAdvertisers match {
      case Some(iso) => afterBots.join(broadcast(iso), Seq("AdvertiserId"), "left_anti")
      case None => afterBots
    }

    val clickIds = clicks.select("BidRequestId").dropDuplicates("BidRequestId")

    afterIsolated
      .join(clickIds.withColumn("has_click", lit(1)), Seq("BidRequestId"), "left")
      .withColumn("has_click", coalesce(col("has_click"), lit(0)))
      .withColumn("impression_time_unix", col("LogEntryTime").cast("long"))
      .withColumn("date", to_date(from_unixtime(col("impression_time_unix"))))
      .withColumn("label", col("has_click").cast("int"))
      .select(
        "BidRequestId",
        "UIID",
        "CampaignId",
        "AdvertiserId",
        "impression_time_unix",
        "date",
        "label"
      )
  }

  def addRecencyFeatures(df: DataFrame): DataFrame = {
    val winUiidCampaign = Window.partitionBy("UIID", "CampaignId").orderBy("impression_time_unix")
    val winUiidCampaignPrev = winUiidCampaign.rowsBetween(Window.unboundedPreceding, -1)
    val winUiid = Window.partitionBy("UIID").orderBy("impression_time_unix")
    val winUiidPrev = winUiid.rowsBetween(Window.unboundedPreceding, -1)

    val withImpr = df.withColumn(
      "uiid_campaign_recency_impression_seconds",
      col("impression_time_unix") - lag(col("impression_time_unix"), 1).over(winUiidCampaign)
    )

    val lastClickUiidCampaign = max(when(col("label") === 1, col("impression_time_unix"))).over(winUiidCampaignPrev)
    val lastClickUiid = max(when(col("label") === 1, col("impression_time_unix"))).over(winUiidPrev)

    withImpr
      .withColumn("uiid_campaign_recency_click_seconds", col("impression_time_unix") - lastClickUiidCampaign)
      .withColumn("uiid_recency_impression_seconds", col("impression_time_unix") - lag(col("impression_time_unix"), 1).over(winUiid))
      .withColumn("uiid_recency_click_seconds", col("impression_time_unix") - lastClickUiid)
  }
}
