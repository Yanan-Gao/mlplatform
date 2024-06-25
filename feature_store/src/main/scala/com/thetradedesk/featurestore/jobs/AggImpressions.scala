package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.datasets._
import com.thetradedesk.featurestore.date
import com.thetradedesk.featurestore.features.Features._
import com.thetradedesk.featurestore.jobs.GenDailyConvertedImpressions.convLookback
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, loadParquetData}
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.{ColumnExtensions, DataSetExtensions}
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import java.time.LocalDate


object AggImpressions extends FeatureStoreAggJob {
  override def jobName: String = "impressions"

  // todo: replace this part by config files
  override def catFeatSpecs: Array[CategoryFeatAggSpecs] = Array(
    CategoryFeatAggSpecs(aggField = "Site", aggWindow = 1, topN = 15, dataType = "string", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "Site", aggWindow = 3, topN = 15, dataType = "string", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "Site", aggWindow = 7, topN = 15, dataType = "string", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "HourOfDay", aggWindow = 1, topN = 15, dataType = "int", cardinality = 25),
    CategoryFeatAggSpecs(aggField = "HourOfDay", aggWindow = 3, topN = 15, dataType = "int", cardinality = 25),
    CategoryFeatAggSpecs(aggField = "HourOfDay", aggWindow = 7, topN = 15, dataType = "int", cardinality = 25),
    CategoryFeatAggSpecs(aggField = "DayOfWeek", aggWindow = 7, topN = 7, dataType = "int", cardinality = 8),
    CategoryFeatAggSpecs(aggField = "DeviceType", aggWindow = 1, topN = 9, dataType = "int", cardinality = 10),
    CategoryFeatAggSpecs(aggField = "DeviceType", aggWindow = 3, topN = 9, dataType = "int", cardinality = 10),
    CategoryFeatAggSpecs(aggField = "DeviceType", aggWindow = 7, topN = 9, dataType = "int", cardinality = 10),
    CategoryFeatAggSpecs(aggField = "DeviceMake", aggWindow = 1, topN = 15, dataType = "int", cardinality = 6002),
    CategoryFeatAggSpecs(aggField = "DeviceMake", aggWindow = 3, topN = 15, dataType = "int", cardinality = 6002),
    CategoryFeatAggSpecs(aggField = "DeviceMake", aggWindow = 7, topN = 15, dataType = "int", cardinality = 6002),
    CategoryFeatAggSpecs(aggField = "DeviceModel", aggWindow = 1, topN = 15, dataType = "int", cardinality = 40002),
    CategoryFeatAggSpecs(aggField = "DeviceModel", aggWindow = 3, topN = 15, dataType = "int", cardinality = 40002),
    CategoryFeatAggSpecs(aggField = "DeviceModel", aggWindow = 7, topN = 15, dataType = "int", cardinality = 40002),
    CategoryFeatAggSpecs(aggField = "RequestLanguages", aggWindow = 1, topN = 15, dataType = "int", cardinality = 5002),
    CategoryFeatAggSpecs(aggField = "RequestLanguages", aggWindow = 3, topN = 15, dataType = "int", cardinality = 5002),
    CategoryFeatAggSpecs(aggField = "RequestLanguages", aggWindow = 7, topN = 15, dataType = "int", cardinality = 5002),
    CategoryFeatAggSpecs(aggField = "MatchedLanguageCode", aggWindow = 1, topN = 15, dataType = "int", cardinality = 352),
    CategoryFeatAggSpecs(aggField = "MatchedLanguageCode", aggWindow = 3, topN = 15, dataType = "int", cardinality = 352),
    CategoryFeatAggSpecs(aggField = "MatchedLanguageCode", aggWindow = 7, topN = 15, dataType = "int", cardinality = 352),
    CategoryFeatAggSpecs(aggField = "Browser", aggWindow = 1, topN = 15, dataType = "int", cardinality = 16),
    CategoryFeatAggSpecs(aggField = "Browser", aggWindow = 3, topN = 15, dataType = "int", cardinality = 16),
    CategoryFeatAggSpecs(aggField = "Browser", aggWindow = 7, topN = 15, dataType = "int", cardinality = 16),
    CategoryFeatAggSpecs(aggField = "RenderingContext", aggWindow = 1, topN = 6, dataType = "int", cardinality = 7),
    CategoryFeatAggSpecs(aggField = "RenderingContext", aggWindow = 3, topN = 6, dataType = "int", cardinality = 7),
    CategoryFeatAggSpecs(aggField = "RenderingContext", aggWindow = 7, topN = 6, dataType = "int", cardinality = 7),
    CategoryFeatAggSpecs(aggField = "InternetConnectionType", aggWindow = 1, topN = 10, dataType = "int", cardinality = 11),
    CategoryFeatAggSpecs(aggField = "InternetConnectionType", aggWindow = 3, topN = 10, dataType = "int", cardinality = 11),
    CategoryFeatAggSpecs(aggField = "InternetConnectionType", aggWindow = 7, topN = 10, dataType = "int", cardinality = 11),
    CategoryFeatAggSpecs(aggField = "OperatingSystemFamily", aggWindow = 1, topN = 7, dataType = "int", cardinality = 8),
    CategoryFeatAggSpecs(aggField = "OperatingSystemFamily", aggWindow = 3, topN = 7, dataType = "int", cardinality = 8),
    CategoryFeatAggSpecs(aggField = "OperatingSystemFamily", aggWindow = 7, topN = 7, dataType = "int", cardinality = 8),
    CategoryFeatAggSpecs(aggField = "OperatingSystem", aggWindow = 1, topN = 15, dataType = "int", cardinality = 72),
    CategoryFeatAggSpecs(aggField = "OperatingSystem", aggWindow = 3, topN = 15, dataType = "int", cardinality = 72),
    CategoryFeatAggSpecs(aggField = "OperatingSystem", aggWindow = 7, topN = 15, dataType = "int", cardinality = 72),
    CategoryFeatAggSpecs(aggField = "ContextualCategories", aggWindow = 1, topN = 15, dataType = "array_long", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "ContextualCategories", aggWindow = 3, topN = 15, dataType = "array_long", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "ContextualCategories", aggWindow = 7, topN = 15, dataType = "array_long", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "Country", aggWindow = 1, topN = 15, dataType = "string", cardinality = 252),
    CategoryFeatAggSpecs(aggField = "Country", aggWindow = 3, topN = 15, dataType = "string", cardinality = 252),
    CategoryFeatAggSpecs(aggField = "Country", aggWindow = 7, topN = 15, dataType = "string", cardinality = 252),
    CategoryFeatAggSpecs(aggField = "Region", aggWindow = 1, topN = 15, dataType = "string", cardinality = 4002),
    CategoryFeatAggSpecs(aggField = "Region", aggWindow = 3, topN = 15, dataType = "string", cardinality = 4002),
    CategoryFeatAggSpecs(aggField = "Region", aggWindow = 7, topN = 15, dataType = "string", cardinality = 4002),
    CategoryFeatAggSpecs(aggField = "Metro", aggWindow = 1, topN = 15, dataType = "string", cardinality = 202),
    CategoryFeatAggSpecs(aggField = "Metro", aggWindow = 3, topN = 15, dataType = "string", cardinality = 202),
    CategoryFeatAggSpecs(aggField = "Metro", aggWindow = 7, topN = 15, dataType = "string", cardinality = 202),
    CategoryFeatAggSpecs(aggField = "City", aggWindow = 1, topN = 15, dataType = "string", cardinality = 150002),
    CategoryFeatAggSpecs(aggField = "City", aggWindow = 3, topN = 15, dataType = "string", cardinality = 150002),
    CategoryFeatAggSpecs(aggField = "City", aggWindow = 7, topN = 15, dataType = "string", cardinality = 150002),
    CategoryFeatAggSpecs(aggField = "Zip", aggWindow = 1, topN = 15, dataType = "string", cardinality = 90002),
    CategoryFeatAggSpecs(aggField = "Zip", aggWindow = 3, topN = 15, dataType = "string", cardinality = 90002),
    CategoryFeatAggSpecs(aggField = "Zip", aggWindow = 7, topN = 15, dataType = "string", cardinality = 90002),
    CategoryFeatAggSpecs(aggField = "ReferrerUrl", aggWindow = 1, topN = 15, dataType = "string", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "ReferrerUrl", aggWindow = 3, topN = 15, dataType = "string", cardinality = 500002),
    CategoryFeatAggSpecs(aggField = "ReferrerUrl", aggWindow = 7, topN = 15, dataType = "string", cardinality = 500002),
  )

  override def conFeatSpecs: Array[ContinuousFeatAggSpecs] = Array(
    ContinuousFeatAggSpecs(aggField = "BidRequestId", aggWindow = 1, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "BidRequestId", aggWindow = 3, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "BidRequestId", aggWindow = 7, aggFunc = AggFunc.Count),
    ContinuousFeatAggSpecs(aggField = "Latitude", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Latitude", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Latitude", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Longitude", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Longitude", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "Longitude", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_week", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_week", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_week", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_week", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_week", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_week", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_day", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_day", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_hour_day", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_day", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_day", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_hour_day", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_hour", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_hour", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_hour", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_hour", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_hour", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_hour", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_day", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_day", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "sin_minute_day", aggWindow = 7, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_day", aggWindow = 1, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_day", aggWindow = 3, aggFunc = AggFunc.Desc),
    ContinuousFeatAggSpecs(aggField = "cos_minute_day", aggWindow = 7, aggFunc = AggFunc.Desc),

  )

  override def ratioFeatSpecs: Array[RatioFeatAggSpecs] = Array(
//    RatioFeatAggSpecs(aggField = "AdjustedBidCPMInUSD", aggWindow = 1, denomField = "FloorPriceInUSD", ratioMetrics = "BidFloorRatio"),
  )

  override def loadInputData(date: LocalDate, lookBack: Int): Dataset[_] = {
    // load impressions from geronimo dataset
    val bidImpressionsS3Path = BidsImpressions.BIDSIMPRESSIONSS3 + "prod/bidsimpressions/"
    val bidsImpressions = loadParquetData[BidsImpressionsSchema](bidImpressionsS3Path, date, lookBack = Some(lookBack), source = Some(GERONIMO_DATA_SOURCE))
    bidsImpressions.filter($"IsImp")
      .withColumn("AdFormat", concat(col("AdWidthInPixels"), lit('x'), col("AdHeightInPixels")))
      .withColumn("IsTracked", when($"UIID".isNotNullOrEmpty && $"UIID" =!= lit("00000000-0000-0000-0000-000000000000"), lit(1)).otherwise(0))
      .withColumn("RenderingContext", $"RenderingContext.value")
      .withColumn("DeviceType", $"DeviceType.value")
      .withColumn("OperatingSystem", $"OperatingSystem.value")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamily.value")
      .withColumn("Browser", $"Browser.value")
      .withColumn("InternetConnectionType", $"InternetConnectionType.value")
      .selectAs[FeatureBidsImpression]
      .withColumn("HourOfDay", hour($"LogEntryTime"))
      .withColumn("DayOfWeek", dayofweek($"LogEntryTime"))
      .withColumnRenamed("UIID", "TDID")
  }
}