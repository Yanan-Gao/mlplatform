package com.thetradedesk

/**
 * Package object for philo package.
 * 
 * Utilities have been organized into focused modules:
 * - Hash functions → com.thetradedesk.geronimo.shared
 * - Feature functions → com.thetradedesk.philo.util.FeatureUtils  
 * - Data functions → com.thetradedesk.philo.util.DataUtils
 * - Filter functions → com.thetradedesk.philo.util.FilterUtils
 * 
 * Import directly from these modules for cleaner dependencies.
 */
package object philo {
  
  // ===== Transform Constants =====
  // 3, 4 for cpc and ctr
  val RoiTypes = Seq(3, 4)
  val Version = 6
  /**
   * Field names to flatten when processing data for model input.
   * These categorical fields are expanded into individual columns.
   */
  val FlattenFields = Set(
    "AdsTxtSellerType", 
    "PublisherType", 
    "DeviceType", 
    "OperatingSystemFamily", 
    "Browser", 
    "RenderingContext", 
    "DoNotTrack"
  )

  /**
   * Column names for sensitive advertiser data handling.
   * Used for filtering and categorizing sensitive/restricted advertisers.
   */
  val SensitiveAdvertiserColumns = List("IsRestricted", "CategoryPolicy")

  // ===== Bot Filtering Constants =====
  
  /**
   * Precomputed impression thresholds for different percentiles.
   * Used for click-bot filtering based on impression patterns.
   * These values are computed offline to avoid expensive percentile calculations at runtime.
   */
  object BotFilteringThresholds {
    val Impression97thPercentile = 18
    val Impression98thPercentile = 25
    val Impression99thPercentile = 40
    val Impression999thPercentile = 100    // 99.9th percentile
    val Impression9999thPercentile = 250   // 99.99th percentile
    val MaxImpressions = 1000
  }

  /**
   * Control points for dynamic CTR thresholding in bot detection.
   * Maps percentile values to corresponding CTR thresholds.
   * Higher impression counts get stricter (lower) CTR thresholds.
   */
  val BotFilteringControlPoints = Map(
    97.0 -> 0.90,
    98.0 -> 0.75,
    99.0 -> 0.60,
    99.9 -> 0.50,
    99.99 -> 0.30
  )

  // ===== Data Source Constants =====
  
  /**
   * S3 path functions for philo dataset locations.
   * Centralized to avoid duplication across ModelInputDataSet and ModelInputUserDataSet.
   */
  object S3Paths {
    val PHILO_S3 = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/processed/"
    val FILTERED = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/filtered/"
    val APAC = (env: String) => f"s3://thetradedesk-mlplatform-us-east-1/features/data/philo/v=1/${env}/apac/"
  }

  /**
   * Column names used for policy-related data processing.
   * Contains advertiser-specific columns for filtering and analysis.
   */
  val PolicyTableColumns = Seq("AdvertiserId")
}