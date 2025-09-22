package com.thetradedesk.philo.transform

import com.thetradedesk.philo.{BotFilteringThresholds, BotFilteringControlPoints}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, expr, lit, max, mean, stddev, sum, udf, when}

/**
 * Bot filtering utilities for click fraud detection and prevention.
 * Implements dynamic CTR thresholding and statistical analysis for bot detection.
 */
object BotFilterTransform {

  /**
   * Filter potential click bots from the dataset.
   * Uses dynamic CTR thresholding based on impression patterns to identify and remove bot traffic.
   * 
   * @param preFilterDataset input DataFrame to filter
   * @return DataFrame with potential bots removed
   */
  def filterBots(preFilterDataset: DataFrame): DataFrame = {

    // Group by UIID to compute user-level click and impression statistics
    val dfUserStats = preFilterDataset.groupBy("UIID")
      .agg(
        sum(when(col("label") === 1, 1).otherwise(0)).alias("num_clicks"),
        count("*").alias("num_impressions")
      )
      .filter(col("num_impressions") > 0) // Only users with impressions

    // Compute CTR (Click-Through Rate) for each user
    val dfUserCtr = dfUserStats
      .withColumn("ctr", col("num_clicks").cast("double") / col("num_impressions").cast("double"))

    // Filter out users with zero clicks to focus on those who clicked
    val dfValidClickData = dfUserCtr.filter(col("num_clicks") > 0)

    // Use control points from package object for dynamic CTR thresholding

    // Create UDF for dynamic CTR threshold calculation
    val dynamicCtrThresholdUdf = udf(
      (numImpressions: Long) => dynamicCtrThreshold(
        numImpressions,
        BotFilteringThresholds.Impression97thPercentile,
        BotFilteringThresholds.Impression98thPercentile,
        BotFilteringThresholds.Impression99thPercentile,
        BotFilteringThresholds.Impression999thPercentile,
        BotFilteringThresholds.Impression9999thPercentile,
        BotFilteringControlPoints,
        BotFilteringThresholds.MaxImpressions
      )
    )

    // Apply the UDF to create a dynamic CTR threshold column
    val dfWithDynamicThreshold = dfValidClickData.withColumn("ctr_threshold", dynamicCtrThresholdUdf(col("num_impressions")))

    // Identify outliers by making sure that the number of impressions is above the 97th percentile and that the CTR is above the dynamic CTR threshold
    val dfOutliers = dfWithDynamicThreshold.filter(
      (col("num_impressions") > BotFilteringThresholds.Impression97thPercentile) && (col("ctr") > col("ctr_threshold"))
    )

    // Get the distinct UIIDs that correspond to outliers/potential clickbots
    val clickbotUiids = dfOutliers
      .select("UIID")
      .distinct()

    // Remove the identified clickbot UIIDs from the original dataset
    val filteredDataWithoutBots = preFilterDataset.join(clickbotUiids, Seq("UIID"), "left_anti")

    filteredDataWithoutBots
  }

  /**
   * Linear interpolation function that calculates interpolated CTR threshold between two points.
   * 
   * @param a starting value
   * @param b ending value
   * @param t interpolation parameter (0.0 to 1.0)
   * @return interpolated value
   */
  private def lerp(a: Double, b: Double, t: Double): Double = {
    a + (b - a) * t
  }

  /**
   * Computes a dynamic CTR threshold based on the number of impressions and predefined control points.
   * Higher impression counts get stricter (lower) CTR thresholds to catch sophisticated bots.
   * 
   * @param numImpressions number of impressions for the user
   * @param threshold97 97th percentile threshold
   * @param threshold98 98th percentile threshold
   * @param threshold99 99th percentile threshold
   * @param threshold999 99.9th percentile threshold
   * @param threshold9999 99.99th percentile threshold
   * @param controlPoints map of percentile to CTR threshold
   * @param maxNumberOfImpressions maximum number of impressions to consider
   * @return dynamic CTR threshold
   */
  private def dynamicCtrThreshold(
    numImpressions: Long,
    threshold97: Long,
    threshold98: Long,
    threshold99: Long,
    threshold999: Long,
    threshold9999: Long,
    controlPoints: Map[Double, Double],
    maxNumberOfImpressions: Long
  ): Double = {
    if (numImpressions >= threshold9999) {
      val t = (numImpressions - threshold9999).toDouble / (maxNumberOfImpressions - threshold9999)
      lerp(controlPoints(99.99), 0.30, t)
    } else if (numImpressions >= threshold999) {
      val t = (numImpressions - threshold999).toDouble / (threshold9999 - threshold999)
      lerp(controlPoints(99.9), controlPoints(99.99), t)
    } else if (numImpressions >= threshold99) {
      val t = (numImpressions - threshold99).toDouble / (threshold999 - threshold99)
      lerp(controlPoints(99), controlPoints(99.9), t)
    } else if (numImpressions >= threshold98) {
      val t = (numImpressions - threshold98).toDouble / (threshold99 - threshold98)
      lerp(controlPoints(98), controlPoints(99), t)
    } else if (numImpressions >= threshold97) {
      val t = (numImpressions - threshold97).toDouble / (threshold98 - threshold97)
      lerp(controlPoints(97), controlPoints(98), t)
    } else {
      1.01 // Threshold above 100% CTR for low impression users (effectively no filtering)
    }
  }

  /**
   * Calculates the number of impressions corresponding to a specific percentile.
   * This method can be used to dynamically compute thresholds but is computationally expensive.
   * 
   * @param dfTotalImpressionCounts DataFrame with impression counts
   * @param percentile percentile to calculate (e.g., 97.0 for 97th percentile)
   * @return impression count at the specified percentile
   */
  def percentileToThreshold(dfTotalImpressionCounts: DataFrame, percentile: Double): Int = {
    val threshold = dfTotalImpressionCounts
      .selectExpr(s"percentile_approx(num_impressions, ${percentile / 100.0}) as threshold")
      .collect()(0)
      .getAs[Int]("threshold")
    threshold
  }
}