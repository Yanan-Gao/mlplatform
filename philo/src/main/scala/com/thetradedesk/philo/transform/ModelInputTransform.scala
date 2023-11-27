package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE, loadModelFeatures}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.philo.{flattenData, schema, shiftModUdf}
import com.thetradedesk.philo.schema.{AdGroupPerformanceModelValueRecord, ClickTrackerRecord, ModelInputRecord,
  CampaignROIGoalDataset, CampaignROIGoalRecord, AdGroupDataset, AdGroupRecord, CreativeLandingPageRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.{CountryFilterRecord}

object ModelInputTransform extends Logger {

  val flatten_set = Set("AdsTxtSellerType","PublisherType", "DeviceType", "OperatingSystemFamily", "Browser", "RenderingContext", "DoNotTrack")



  // return training input based on bidimps combined dataset
  // if filterresults = true, it will be based on either AdGroupId (from s3 adgroup dataset) or from
  // Country (from s3 country filter dataset, or both
  // if it is landingpage, then it will be adgroup with CPC/CTR goal and adding landingpage
  // as the feature
  def transform(clicks: Dataset[ClickTrackerRecord],
                adgroup: Dataset[AdGroupRecord],
                bidsImpsDat: Dataset[BidsImpressionsSchema],
                performanceModelValues: Dataset[AdGroupPerformanceModelValueRecord],
                roiFilter: Option[Seq[Int]],
                creativeLandingPage: Option[Dataset[CreativeLandingPageRecord]],
                countryFilter: Option[Dataset[CountryFilterRecord]],
                filterResults: Boolean = false,
                keptCols: Seq[String] = Seq("CampaignId", "AdGroupId", "Country"),
                modelFeatures: Seq[ModelFeature]): (DataFrame, DataFrame) = {
    val (clickLabels, bidsImpsPreJoin) = hashBidAndClickLabels(clicks, bidsImpsDat)
    val preFilteredData = preFilterJoin(clickLabels, adgroup, bidsImpsPreJoin, performanceModelValues, keptCols)
    val filteredData = preFilteredData
      .transform(ds => if (filterResults) {filterDataset(ds, roiFilter, countryFilter)} else ds)
      .transform(ds => creativeLandingPage.map(clp => ModelInputTransform.matchLandingPage(ds, clp)).getOrElse(ds))
    val flatten = flattenData(filteredData.toDF, flatten_set)
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

  def filterDataset(preFilterDataset: DataFrame,
                    roiFilter: Option[Seq[Int]],
                    countryFilter: Option[Dataset[CountryFilterRecord]]
                   ): DataFrame = {
    preFilterDataset
      .transform(ds => roiFilter.map(filter => ds.filter(col("ROIGoalTypeId").isin(filter: _*))).getOrElse(ds))
      .transform(ds => countryFilter.map(filter => ds.join(filter, Seq("Country"))).getOrElse(ds))
  }

  def matchLandingPage(filteredData: DataFrame,
                       creativeLandingPage: Dataset[CreativeLandingPageRecord]): DataFrame = {
    val hashedData = filteredData.withColumn("hashedCreativeId", xxhash64(col("CreativeId")))
    val hashedCreativeLanding = creativeLandingPage.withColumn("hashedCreativeId", xxhash64(col("CreativeId"))).drop("CreativeID")
    hashedData.join(hashedCreativeLanding, Seq("hashedCreativeId")).drop("hashedCreativeId")
  }


  def preFilterJoin(clickLabels: DataFrame,
                    adgroup: Dataset[AdGroupRecord],
                    bidsImpsPreJoin: DataFrame,
                    performanceModelValues: Dataset[AdGroupPerformanceModelValueRecord],
                    keptCols: Seq[String]): DataFrame = {
    val baseJoin = bidsImpsPreJoin.join(clickLabels, Seq("BidRequestIdHash"), "leftouter")
      .join(performanceModelValues, Seq("AdGroupId"), "leftouter")
      .join(adgroup, Seq("AdGroupId"), "leftouter")
      .withColumn("label", when(col("label").isNull, 0).otherwise(1))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
      .withColumn("IsTestAdGroup", when(col("ModelType") === 1 && col("ModelVersion") == 1, 1).otherwise(0))
    // add unhashed columns to output data
    val addKeptCols = keptCols.foldLeft(baseJoin) { (tempDF, colName) =>
      tempDF.withColumn(s"original$colName", col(colName))
    }
    addKeptCols
  }

  def getHashedData(flatten: Dataset[ModelInputRecord], modelFeatures: Seq[ModelFeature]): DataFrame ={
    // todo: we need a better way to track these fields
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ Seq("label", "BidRequestId", "OriginalAdGroupId", "OriginalCountry", "IsTestAdGroup").map(col)

    flatten.select(selectionQuery: _*)
  }

}
