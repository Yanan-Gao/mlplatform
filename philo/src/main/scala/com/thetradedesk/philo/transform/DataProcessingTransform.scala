package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.philo.schema.ClickTrackerRecord
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, when, size}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._

/**
 * Data processing and analysis utilities for model input preparation.
 * Handles label creation, user data features, and data preparation operations.
 */
object DataProcessingTransform {

  /**
   * Add bid and click labels to datasets.
   * Prepares click data with labels and filters impression data.
   * 
   * @param clicks dataset of click tracker records
   * @param bidsImpsDat dataset of bids/impressions data
   * @return tuple of (labeled clicks dataset, filtered bids/impressions dataset)
   */
  def addBidAndClickLabels(clicks: Dataset[ClickTrackerRecord],
                           bidsImpsDat: Dataset[BidsImpressionsSchema]): (Dataset[ClickTrackerRecord], Dataset[BidsImpressionsSchema]) = {

    val clickLabels = clicks.distinct().withColumn("label", lit(1))
      .as[ClickTrackerRecord]

    val bidsImpsPreJoin = bidsImpsDat
      // is imp is a boolean
      .filter(col("IsImp"))
      .as[BidsImpressionsSchema]

    (clickLabels, bidsImpsPreJoin)
  }

  /**
   * Add user data features to the dataset.
   * Creates features related to user data segments and applies optional bot filtering.
   * 
   * @param data input DataFrame
   * @param filterClickBots whether to apply click bot filtering
   * @return DataFrame with user data features added
   */
  def addUserDataFeatures(data: DataFrame, filterClickBots: Boolean = false): DataFrame = {

    /*
    This function adds MatchedSegments, HasUserData, & UserDataLength
    --- MatchedSegments is an array of TargetingDataIds which is not supported by .csv
    --- Need to manually map every segment to its own "UserData_Column{i}"
     */

    // Filter data if removing click bots
    val filteredData = if (filterClickBots) {
      BotFilterTransform.filterBots(data)
    } else {
      data
    }

    val updatedData = filteredData
      .withColumn("HasUserData", 
        when($"MatchedSegments".isNull || size($"MatchedSegments") === lit(0), lit(0)).otherwise(lit(1)))
      .withColumn("UserDataLength", 
        when($"UserSegmentCount".isNull, lit(0.0)).otherwise($"UserSegmentCount" * lit(1.0)))
      .withColumn("UserData", 
        when($"HasUserData" === lit(0), lit(null)).otherwise($"MatchedSegments"))
      .withColumn("UserDataOptIn", lit(1))

    // Return updated dataset with user data features
    updatedData
  }
}