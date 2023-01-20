package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, loadModelFeatures}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.philo.{flattenData, schema, shiftModUdf}
import com.thetradedesk.philo.schema.{AdGroupPerformanceModelValueRecord, ClickTrackerRecord, ModelInputRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.{AdGroupFilterRecord, CountryFilterRecord}

object ModelInputTransform extends Logger {

  val flatten_set = Set("AdsTxtSellerType","PublisherType", "DeviceType", "OperatingSystemFamily", "Browser", "RenderingContext", "DoNotTrack")



  // return training input based on bidimps combined dataset
  // if filterresults = true, adgroupfilter must be provided & output will be filtered.
  def transform(clicks: Dataset[ClickTrackerRecord],
                bidsImpsDat: Dataset[BidsImpressionsSchema],
                performanceModelValues: Dataset[AdGroupPerformanceModelValueRecord],
                adGroupFilter: Option[Dataset[AdGroupFilterRecord]],
                countryFilter: Option[Dataset[CountryFilterRecord]],
                filterResults: Boolean = false,
                modelFeatures: Seq[ModelFeature]): (DataFrame, DataFrame) = {

    val (clickLabels, bidsImpsPreJoin) = hashBidAndClickLabels(clicks, bidsImpsDat)

    val joinedData = joinDatasets(clickLabels, bidsImpsPreJoin, performanceModelValues, adGroupFilter, countryFilter, filterResults)

    val flatten = flattenData(joinedData.toDF, flatten_set)
      .selectAs[ModelInputRecord]

    // Get the unique labels with count for each.
    val label_counts = flatten.groupBy("label").count()

    val hashedData = getHashedData(flatten, modelFeatures)

    (hashedData, label_counts)
  }

  def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _) => col(name).alias(name)
    }.toArray
  }

  def hashBidAndClickLabels(clicks: Dataset[ClickTrackerRecord],
                            bidsImpsDat: Dataset[BidsImpressionsSchema]) : (DataFrame, DataFrame) = {

    val clickLabels = clicks.withColumn("label", lit(1))
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))
      .drop("BidRequestId")

    val bidsImpsPreJoin = bidsImpsDat
      // is imp is a boolean
      .filter(col("IsImp"))
      .withColumn("BidRequestIdHash" , xxhash64(col("BidRequestId")))

    (clickLabels, bidsImpsPreJoin)
  }

  def joinDatasets(clickLabels: DataFrame,
                   bidsImpsPreJoin: DataFrame,
                   performanceModelValues: Dataset[AdGroupPerformanceModelValueRecord],
                   adGroupIdFilter: Option[Dataset[AdGroupFilterRecord]] = None,
                   countryFilter: Option[Dataset[CountryFilterRecord]] = None,
                   filterResults: Boolean = false): DataFrame = {
    bidsImpsPreJoin.join(clickLabels, Seq("BidRequestIdHash"), "leftouter")
      .join(performanceModelValues, Seq("AdGroupId"), "leftouter")
      .withColumn("label", when(col("label").isNull, 0).otherwise(1))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .withColumn("IsTestAdGroup", when(col("ModelType") === 1 && col("ModelVersion") == 1, 1).otherwise(0))
      // add unhashed columns to output dataset
      .withColumn("OriginalAdGroupId", $"AdGroupId")
      .withColumn("OriginalCountry", $"Country")
      // filter results if we have a filter
      .transform(ds => if (filterResults && adGroupIdFilter.isDefined) {
          ds.join(adGroupIdFilter.get, Seq("AdGroupId"))
        } else if (filterResults && countryFilter.isDefined) {
          ds.join(countryFilter.get, Seq("Country"))
        } else ds)
  }

  def getHashedData(flatten: Dataset[ModelInputRecord], modelFeatures: Seq[ModelFeature]): DataFrame ={
    // todo: we need a better way to track these fields
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ Seq("label", "BidRequestId", "OriginalAdGroupId", "OriginalCountry", "IsTestAdGroup").map(col)

    flatten.select(selectionQuery: _*)
  }

}
