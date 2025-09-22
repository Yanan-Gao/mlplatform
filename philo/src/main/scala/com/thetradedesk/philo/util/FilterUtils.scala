package com.thetradedesk.philo.util

import com.thetradedesk.philo.schema.{AdGroupRecord, CampaignROIGoalDataSet, CountryFilterRecord}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.coalesce
import java.time.LocalDate

/**
 * Filtering utilities for data processing.
 */
object FilterUtils {

  /**
   * Filter ad groups based on ROI goal types.
   * Joins with campaign ROI goals and filters based on goal types.
   * 
   * @param date date for filtering
   * @param adgroup ad group dataset
   * @param roi_types sequence of ROI goal type IDs to filter by
   * @param spark implicit SparkSession
   * @return filtered ad group dataset
   */
  def getAdGroupFilter(date: LocalDate, adgroup: Dataset[AdGroupRecord],
                       roi_types: Seq[Int])(implicit spark: SparkSession): Dataset[AdGroupRecord] = {
    import spark.implicits._
    val campaignGoal = CampaignROIGoalDataSet().readLatestPartitionUpTo(date, isInclusive = true)
      // get primary goal only
      .filter($"Priority" === 1)
    adgroup.as("ag").join(campaignGoal.as("cg"), Seq("CampaignId"), "left")
      // consider campaign goal first, fall back to adgroup goal if not defined
      .withColumn("goal", coalesce($"cg.ROIGoalTypeId", $"ag.ROIGoalTypeId"))
      .filter($"goal".isin(roi_types: _*))
      .select("ag.*")
      .as[AdGroupRecord]
  }

  /**
   * Get country filter from file path.
   * Reads a CSV file containing country codes for filtering.
   * 
   * @param countryFilePath path to country filter CSV file
   * @param spark implicit SparkSession
   * @return dataset of country filter records
   * @throws Exception if file doesn't exist or path is empty
   */
  def getCountryFilter(countryFilePath: String)(implicit spark: SparkSession): Dataset[CountryFilterRecord] = {
    import spark.implicits._
    // currently adgroup filter are moved into the pipeline without the csv adgorup list
    // it is reflected in ROI goals set to CPC and CTR in roi_types
    // Therefore, if the coutryFilePath is not empty and it is adgroup based filtering
    // then it will select the CPC and CTR adgroups within the given countries
    // otherwise, it will be just based on country
    if (!countryFilePath.isEmpty) {
      if (com.thetradedesk.spark.util.io.FSUtils.fileExists(countryFilePath)) {
        spark.read.format("csv")
          .load(countryFilePath)
          // single column is unnamed
          .withColumn("Country", $"_c0")
          .select("Country").as[CountryFilterRecord]
      } else {
        throw new Exception(f"Country filter file does not exist at ${countryFilePath}")
      }
    } else {
      throw new Exception(f"Country file path is empty")
    }
  }
}