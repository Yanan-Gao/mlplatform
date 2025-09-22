package com.thetradedesk.philo.transform

import com.thetradedesk.philo.schema.{AdGroupRecord, PartnerExclusionRecord, SensitiveAdvertiserRecord, CreativeLandingPageRecord, CountryFilterRecord}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{broadcast, col, concat_ws, lit, when, xxhash64}

/**
 * Data filtering and joining utilities for model input processing.
 * Handles dataset filtering, exclusion flags, and data joining operations.
 */
object FilterTransform {

  /**
   * Add partner exclusion flags to the dataset.
   * Marks rows as excluded (1) or not excluded (0) based on partner exclusion list.
   * 
   * @param df input DataFrame
   * @param partnerExclusionList optional dataset of partner exclusions
   * @return DataFrame with exclusion flags added
   */
  def addExclusionFlag(df: DataFrame, partnerExclusionList: Option[Dataset[PartnerExclusionRecord]]): DataFrame = {
    // Check if the advertiser exclusion list is defined
    if (partnerExclusionList.isDefined) {
      val exclusionList = partnerExclusionList.get
      
      // Broadcast the small exclusion list for efficient join
      // Partner exclusion lists are typically small (hundreds to thousands of entries)
      // while the main dataset can be millions/billions of rows
      val broadcastExclusion = broadcast(exclusionList.withColumn("excluded", lit(1)))
      
      // Join with exclusion list and set the 'excluded' flag
      df.join(broadcastExclusion, Seq("PartnerId"), "leftouter")
        .withColumn("excluded", when(col("excluded").isNull, 0).otherwise(1))
    } else {
      // If no exclusion list, mark all rows as not excluded (excluded = 0)
      df.withColumn("excluded", lit(0))
    }
  }

  /**
   * Add restricted advertiser flags to the dataset.
   * Marks rows as restricted (1) or not restricted (0) based on sensitive advertiser list.
   * 
   * @param df input DataFrame
   * @param sensitiveAdvertiserData optional dataset of sensitive advertisers
   * @return DataFrame with restriction flags added
   */
  def addRestrictedFlag(df: DataFrame, sensitiveAdvertiserData: Option[Dataset[SensitiveAdvertiserRecord]]): DataFrame = {
    // Check if sensitive advertiser list is defined
    if (sensitiveAdvertiserData.isDefined) {
      val sensitiveAdvertiserDf = sensitiveAdvertiserData.get
        .filter(col("IsRestricted") === 1)
      
      // Broadcast the small sensitive advertiser list for efficient join
      // Sensitive advertiser lists are typically small (hundreds to thousands of entries)
      // while the main dataset can be millions/billions of rows
      val broadcastSensitive = broadcast(sensitiveAdvertiserDf)
      
      // Join with sensitive advertiser list and set the IsRestricted flag
      df.join(broadcastSensitive, Seq("AdvertiserId"), "leftouter")
        .withColumn("IsRestricted", when(col("IsRestricted").isNull, 0).otherwise(1))
    } else df
  }

  /**
   * Filter dataset by ad group and country criteria.
   * Applies optional filtering based on ad group and country restrictions.
   * 
   * @param preFilterDataset input DataFrame to filter
   * @param adgroup ad group dataset for joining
   * @param filterAdGroup whether to apply strict ad group filtering (inner join vs left outer)
   * @param countryFilter optional country filter dataset
   * @return filtered DataFrame
   */
  def filterDataset(preFilterDataset: DataFrame,
                    adgroup: Dataset[AdGroupRecord],
                    filterAdGroup: Boolean = false,
                    countryFilter: Option[Dataset[CountryFilterRecord]]): DataFrame = {
    
    // Broadcast adgroup dataset for efficient join
    // AdGroup datasets are typically much smaller than impression/click data
    val broadcastAdgroup = broadcast(adgroup)
    
    preFilterDataset
      .transform(
        ds => // if filterAdGroup, adgroup is not just additional info, it is also used to filter adgroup for the training data set
        if (filterAdGroup) {ds.join(broadcastAdgroup, Seq("AdGroupId", "CampaignId"), "inner")}
        else {ds.join(broadcastAdgroup, Seq("AdGroupId", "CampaignId"), "leftouter")}
      )
      .transform(ds => countryFilter.map(filter => 
        // Broadcast country filter - typically very small (list of allowed countries)
        ds.join(broadcast(filter), Seq("Country"))
      ).getOrElse(ds))
  }

  /**
   * Match and join creative landing page data.
   * Joins the dataset with creative landing page information using hashed creative IDs.
   * 
   * @param filteredData input DataFrame
   * @param creativeLandingPage dataset containing creative landing page information
   * @return DataFrame with landing page data joined
   */
  def matchLandingPage(filteredData: DataFrame,
                       creativeLandingPage: Dataset[CreativeLandingPageRecord]): DataFrame = {
    val hashedData = filteredData.withColumn("hashedCreativeId", xxhash64(col("CreativeId")))
    val hashedCreativeLanding = creativeLandingPage
      .withColumn("hashedCreativeId", xxhash64(col("CreativeId")))
      .drop("CreativeID")
    
    // Broadcast creative landing page data - typically small dataset
    val broadcastCreativeLanding = broadcast(hashedCreativeLanding)
    hashedData.join(broadcastCreativeLanding, Seq("hashedCreativeId")).drop("hashedCreativeId")
  }

  /**
   * Pre-filter join operation between clicks and bids/impressions data.
   * Joins bid/impression data with click labels and creates necessary derived columns.
   * 
   * @param clickLabels dataset of click tracker records with labels
   * @param bidsImpsPreJoin dataset of bids/impressions data
   * @return DataFrame with joined data and derived columns
   */
  def preFilterJoin(clickLabels: Dataset[_], bidsImpsPreJoin: Dataset[_]): DataFrame = {
    bidsImpsPreJoin.toDF().join(clickLabels.toDF(), Seq("BidRequestId"), "leftouter")
      .withColumn("label", when(col("label").isNull, 0).otherwise(col("label")))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
  }
}