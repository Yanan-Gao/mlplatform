package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.bidsimpression.schema.BidsImpressionsSchema
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.logging.Logger
import com.thetradedesk.philo.{flattenData, schema, shiftModUdf}
import com.thetradedesk.philo.schema.{ClickTrackerRecord, ModelInputRecord}
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, when, xxhash64}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import job.{AdGroupFilterRecord, CountryFilterRecord}

object ModelInputTransform extends Logger {

  val flatten_set = Seq("AdsTxtSellerType","PublisherType", "DeviceType", "OperatingSystemFamily", "Browser", "RenderingContext", "DoNotTrack")

  val STRING_FEATURE_TYPE = "string"
  val INT_FEATURE_TYPE = "int"
  val FLOAT_FEATURE_TYPE = "float"

  val modelFeatures: Array[ModelFeature] = Array(
    // and this one
    ModelFeature("AdFormat", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("AdgroupId", STRING_FEATURE_TYPE, Some(1002), 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("AdsTxtSellerType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("PublisherType", INT_FEATURE_TYPE, Some(7), 0),

    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("Metro", STRING_FEATURE_TYPE, Some(302), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(75002), 0),
    ModelFeature("Zip", STRING_FEATURE_TYPE, Some(90002), 0),

    ModelFeature("Browser", INT_FEATURE_TYPE, Some(20), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(1002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(10002), 0),
    ModelFeature("DeviceType", INT_FEATURE_TYPE, Some(9), 0),

    ModelFeature("CreativeId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("DoNotTrack", INT_FEATURE_TYPE, Some(2), 0), // need jiaxing input on that
    ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("OperatingSystemFamily", INT_FEATURE_TYPE, Some(10), 0),
    ModelFeature("RenderingContext", INT_FEATURE_TYPE, Some(6), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(502), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(350002), 0),
    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(15002), 0),
    ModelFeature("SupplyVendorSiteId", STRING_FEATURE_TYPE, Some(960002), 0),
    ModelFeature("MatchedFoldPosition", INT_FEATURE_TYPE, Some(3), 0),
    ModelFeature("UserHourOfWeek", INT_FEATURE_TYPE, Some(24 * 7 + 2), 0),

    ModelFeature("sin_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("latitude", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("longitude", FLOAT_FEATURE_TYPE, None, 0),

    // should this have a cardinality?
    ModelFeature("PrivateContractId", STRING_FEATURE_TYPE, Some(10002), 0)

  )

  // return training input based on bidimps combined dataset
  // if filterresults = true, adgroupfilter must be provided & output will be filtered.
  def transform(clicks: Dataset[ClickTrackerRecord],
                bidsImpsDat: Dataset[BidsImpressionsSchema],
                adGroupFilter: Option[Dataset[AdGroupFilterRecord]],
                countryFilter: Option[Dataset[CountryFilterRecord]],
                filterResults: Boolean = false): (DataFrame, DataFrame) = {

    val (clickLabels, bidsImpsPreJoin) = hashBidAndClickLabels(clicks, bidsImpsDat)

    val joinedData = joinDatasets(clickLabels, bidsImpsPreJoin, adGroupFilter, countryFilter, filterResults)

    val flatten = flattenData(joinedData.toDF, flatten_set)
      .selectAs[ModelInputRecord]

    // Get the unique labels with count for each.
    val label_counts = flatten.groupBy("label").count()

    val hashedData = getHashedData(flatten)

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
                   adGroupIdFilter: Option[Dataset[AdGroupFilterRecord]] = None,
                   countryFilter: Option[Dataset[CountryFilterRecord]] = None,
                   filterResults: Boolean = false): DataFrame = {
    bidsImpsPreJoin.join(clickLabels, Seq("BidRequestIdHash"), "leftouter")
      .withColumn("label", when(col("label").isNull, 0).otherwise(1))
      .withColumn("AdFormat", concat_ws("x", col("AdWidthInPixels"), col("AdHeightInPixels")))
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

  def getHashedData(flatten: Dataset[ModelInputRecord]): DataFrame ={
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ Array(col("label"), col("BidRequestId"))

    flatten.select(selectionQuery: _*)
  }


}
