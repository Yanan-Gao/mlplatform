package com.thetradedesk.frequency.transform

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.thetradedesk.geronimo.shared.explicitDatePart
import org.apache.spark.sql.functions._

object SevenDayUiidAggregationTransform {

  def buildSevenDayViews(
      spark: SparkSession,
      readEnv: String,
      subset: String,
      runDate: LocalDate,
      lookbackDays: Int,
      dailyRoot: String,
      aggregateSets: Seq[Seq[Int]]
  ): (DataFrame, DataFrame, Seq[Int]) = {
    require(lookbackDays > 0, "lookback_days must be positive")

    var perUiidCampaign: Option[DataFrame] = None
    var perUiid: Option[DataFrame] = None
    val loadedOffsets = scala.collection.mutable.ArrayBuffer.empty[Int]

    for (offset <- 1 to lookbackDays) {
      val currentDay = runDate.minusDays(offset.toLong)
      val dailyDf = readDailySubset(spark, dailyRoot, subset, currentDay)
      val filtered = dailyDf.where(col("UIID").isNotNull)

      val dailyPerUiidCampaign = renameMetrics(
        filtered.select("UIID", "CampaignId", "AdvertiserId", "impression_count", "click_count"),
        offset,
        prefix = ""
      )

      val dailyPerUiid = renameMetrics(
        aggregateByUiid(filtered),
        offset,
        prefix = "uiid_"
      )

      perUiidCampaign = Some(joinWithKey(perUiidCampaign, dailyPerUiidCampaign, Seq("UIID", "CampaignId", "AdvertiserId")))
      perUiid = Some(joinWithKey(perUiid, dailyPerUiid, Seq("UIID")))
      loadedOffsets += offset
    }

    if (loadedOffsets.isEmpty) {
      throw new RuntimeException("No daily aggregation partitions were loaded for the requested window")
    }

    val perUiidCampaignFilled = fillMissingMetrics(perUiidCampaign.get)
    val perUiidFilled = fillMissingMetrics(perUiid.get)

    val perUiidCampaignWithSets = applyAggregateSetsIfNeeded(
      perUiidCampaignFilled,
      baseImpressionPrefix = "impression_count_d",
      baseClickPrefix = "click_count_d",
      aggImpressionPrefix = "impression_count_offsets_",
      aggClickPrefix = "click_count_offsets_",
      aggregateSets
    )

    val perUiidWithSets = applyAggregateSetsIfNeeded(
      perUiidFilled,
      baseImpressionPrefix = "uiid_impression_count_d",
      baseClickPrefix = "uiid_click_count_d",
      aggImpressionPrefix = "uiid_impression_count_offsets_",
      aggClickPrefix = "uiid_click_count_offsets_",
      aggregateSets
    )

    (perUiidCampaignWithSets, perUiidWithSets, loadedOffsets.toSeq)
  }

  private def readDailySubset(
      spark: SparkSession,
      root: String,
      subset: String,
      day: LocalDate
  ): DataFrame = {
    val normalizedRoot = root.stripSuffix("/")
    val path = s"$normalizedRoot/$subset/${explicitDatePart(day)}"
    spark.read.parquet(path)
  }

  private def aggregateByUiid(df: DataFrame): DataFrame = {
    df.groupBy("UIID")
      .agg(
        sum("impression_count").alias("impression_count"),
        sum("click_count").alias("click_count")
      )
  }

  private def renameMetrics(df: DataFrame, offset: Int, prefix: String): DataFrame = {
    val suffix = s"d$offset"
    df.withColumnRenamed("impression_count", s"${prefix}impression_count_$suffix")
      .withColumnRenamed("click_count", s"${prefix}click_count_$suffix")
  }

  private def joinWithKey(base: Option[DataFrame], update: DataFrame, keys: Seq[String]): DataFrame = {
    base match {
      case Some(existing) => existing.join(update, keys, "outer")
      case None => update
    }
  }

  private def fillMissingMetrics(df: DataFrame): DataFrame = {
    val metricCols = df.columns.filter(_.contains("_count"))
    if (metricCols.isEmpty) df else df.na.fill(0, metricCols)
  }

  private def applyAggregateSetsIfNeeded(
      df: DataFrame,
      baseImpressionPrefix: String,
      baseClickPrefix: String,
      aggImpressionPrefix: String,
      aggClickPrefix: String,
      aggregateSets: Seq[Seq[Int]]
  ): DataFrame = {
    if (aggregateSets.isEmpty) {
      df
    } else {
      val withImps = applyAggregateSets(df, baseImpressionPrefix, aggImpressionPrefix, aggregateSets)
      applyAggregateSets(withImps, baseClickPrefix, aggClickPrefix, aggregateSets)
    }
  }

  private def applyAggregateSets(
      df: DataFrame,
      basePrefix: String,
      columnPrefix: String,
      aggregateSets: Seq[Seq[Int]]
  ): DataFrame = {
    aggregateSets.foldLeft(df) { (acc, offsets) =>
      val sortedOffsets = offsets.sorted
      val targetCol = s"$columnPrefix${sortedOffsets.mkString("_")}"

      val sourceColumns = sortedOffsets.map { offset =>
        val colName = s"$basePrefix$offset"
        if (!acc.columns.contains(colName)) {
          throw new IllegalArgumentException(
            s"Requested aggregate offset $offset missing column '$colName'. Increase lookback_days or adjust aggregate_sets."
          )
        }
        col(colName)
      }

      val sumExpr = sourceColumns.reduce(_ + _)
      acc.withColumn(targetCol, sumExpr)
    }
  }

  
}
