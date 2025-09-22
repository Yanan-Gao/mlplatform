package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.philo.util.DataUtils.{addOriginalCols, debugInfo, flattenData}
import com.thetradedesk.philo.FlattenFields
import com.thetradedesk.philo.schema.{AdGroupRecord, ClickTrackerRecord, CreativeLandingPageRecord, PartnerExclusionRecord, SensitiveAdvertiserRecord, CountryFilterRecord}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Main model input transformation orchestrator.
 * Coordinates the overall transformation pipeline by delegating to specialized transform modules.
 */
object ModelInputTransform extends Logger {

  /**
   * Main transformation pipeline for model input preparation.
   * Orchestrates the complete data processing workflow from raw data to model-ready features.
   * 
   * @param clicks dataset of click tracker records
   * @param adgroup dataset of ad group records
   * @param bidsImpsDat dataset of bids/impressions data
   * @param filterAdGroup whether to apply strict ad group filtering
   * @param creativeLandingPage optional creative landing page data
   * @param countryFilter optional country filter data
   * @param keptCols columns to preserve in original form
   * @param bidRequestFeatures bid request features to process
   * @param adGroupFeatures ad group features to process
   * @param seqHashFields sequence/array features to process
   * @param addUserData whether to include user data features
   * @param filterClickBots whether to apply click bot filtering
   * @param partnerExclusionList optional partner exclusion data
   * @param addCols additional columns to include
   * @param sensitiveAdvertiserData optional sensitive advertiser data
   * @param debug whether to output debug information
   * @return cached processed data DataFrame (ready for writing and label counting)
   */
  def transform(clicks: Dataset[ClickTrackerRecord],
                adgroup: Dataset[AdGroupRecord],
                bidsImpsDat: Dataset[BidsImpressionsSchema],
                filterAdGroup: Boolean,
                creativeLandingPage: Option[Dataset[CreativeLandingPageRecord]],
                countryFilter: Option[Dataset[CountryFilterRecord]],
                keptCols: Seq[String],
                bidRequestFeatures: Seq[ModelFeature],
                adGroupFeatures: Seq[ModelFeature],
                seqHashFields: Seq[ModelFeature],
                addUserData: Boolean,
                filterClickBots: Boolean,
                partnerExclusionList: Option[Dataset[PartnerExclusionRecord]],
                addCols: Seq[String],
                sensitiveAdvertiserData: Option[Dataset[SensitiveAdvertiserRecord]],
                debug: Boolean): DataFrame = {
    
    // Step 1: Prepare base data with labels
    val (clickLabels, bidsImpsPreJoin) = DataProcessingTransform.addBidAndClickLabels(clicks, bidsImpsDat)
    val preFilteredData = FilterTransform.preFilterJoin(clickLabels, bidsImpsPreJoin)
    
    // Step 2: Apply filtering and enrichment transformations
    val filteredData = preFilteredData
      .transform(ds => FilterTransform.addExclusionFlag(ds, partnerExclusionList))
      .transform(ds => FilterTransform.addRestrictedFlag(ds, sensitiveAdvertiserData))
      .transform(ds => FilterTransform.filterDataset(ds, adgroup, filterAdGroup, countryFilter))
      .transform(ds => creativeLandingPage.map(clp => FilterTransform.matchLandingPage(ds, clp)).getOrElse(ds))

    val modelFeatures = bidRequestFeatures ++ adGroupFeatures

    // Step 3: Add original columns and debug output
    val (addKeptCols, originalColNames) = addOriginalCols(keptCols, filteredData.toDF)
    if (debug) {
      debugInfo("PreFilteredData", preFilteredData)
      debugInfo("filteredData", filteredData)
      debugInfo("addKeptCols", addKeptCols)
    }
    
    // Step 4: Process features and generate final output
    // Apply user data features if requested, otherwise use the data as-is
    val data = if (addUserData) {
      DataProcessingTransform.addUserDataFeatures(addKeptCols, filterClickBots)
    } else {
      addKeptCols
    }
    
    val flatten = flattenData(data, FlattenFields)
    
    val hashedData = FeatureTransform.getHashedData(flatten, modelFeatures, seqHashFields, originalColNames, addCols)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)  // Safe for all dataset sizes
    
    if (debug) {
      debugInfo("hashedData", hashedData)
    }
    
    hashedData
  }

  /**
   * Generate label counts from cached training data.
   * This function triggers cache materialization when called.
   * 
   * @param cachedData cached training data from transform()
   * @param sensitiveAdvertiserData optional sensitive advertiser data (affects grouping columns)
   * @return DataFrame with label counts grouped by label, excluded, and optionally CategoryPolicy
   */
  def countLabels(cachedData: DataFrame,
                  sensitiveAdvertiserData: Option[Dataset[SensitiveAdvertiserRecord]]): DataFrame = {
    // Generate label counts - include CategoryPolicy if sensitive advertiser data is available
    if (sensitiveAdvertiserData.isEmpty) {
      cachedData.groupBy("label", "excluded").count()
    } else {
      cachedData.groupBy("label", "excluded", "CategoryPolicy").count()
    }
  }

  // All helper methods have been moved to specialized transform modules:
  // - FeatureTransform: Feature processing and hashing
  // - FilterTransform: Data filtering and joining
  // - DataProcessingTransform: Data processing and analysis
  // - BotFilterTransform: Bot filtering with utility functions
}