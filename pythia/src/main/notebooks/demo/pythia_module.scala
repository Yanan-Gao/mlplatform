// Databricks notebook source
import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, explicitDatePart, paddedDatePart, parquetDataPaths, loadParquetData, shiftMod, shiftModUdf}
import com.thetradedesk.spark.util.io.FSUtils

import com.thetradedesk.streaming.records.rtb.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, InternetConnectionTypeLookupRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord, OSLookupRecord}

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.logging.Logger

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.time.LocalDate

// COMMAND ----------

// MAGIC %md Package

// COMMAND ----------

def shiftModArray(hashList: Seq[Long], cardinality: Int): Array[Int] = {
   hashList.map(hashValue => shiftMod(hashValue, cardinality)).toArray
}

def shiftModArrayUdf = udf(shiftModArray _)

// NOTE: you need the linkedin-spark library for "tfrecord" to work (vs. "tfrecords")
def writeData(df: DataFrame, outputPath: String, outputPrefix: String, date: LocalDate, partitions: Int, isTFRecord: Boolean = true): Unit = {

  // note the date part is year=yyyy/month=m/day=d/
  if (isTFRecord) {
    df.withColumn("index", monotonically_increasing_id())
      .coalesce(partitions)
      .write.mode(SaveMode.Overwrite)
      .partitionBy("Country")
      .format("tfrecord")
      .option("recordType", "Example")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"$outputPath/$outputPrefix/Date=$date/")
  } else {
    df.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(s"$outputPath/$outputPrefix/Date=$date/")
  }
}

def flattenData(data: DataFrame, flatten_set: Seq[String]): DataFrame = {
  data.select(
    data.columns.map(
      c => if (flatten_set.contains(c))
        col(s"${c}.value").alias(c).alias(c)
      else col(c)
    ): _*
  )
}

// COMMAND ----------

// MAGIC %md Schema

// COMMAND ----------

case class ModelInputRecord(
    // BidRequestId: String,
    // AdFormat: String,
    AdGroupId: String,
    AdvertiserId: String,
    AdsTxtSellerType: String,
    PublisherType: String,
    Metro: String,
    Zip: String,
    City: String,
    Country: String,
    Region: String,
    Browser: String,
    DeviceMake: String,
    DeviceModel: String,
    DeviceType: String,
    CreativeId: String,
    DoNotTrack: Int,
    ImpressionPlacementId: String,
    OperatingSystemFamily: String,
    RenderingContext: String,
    RequestLanguages: String,
    Site: String,
    SupplyVendor: String,
    SupplyVendorPublisherId: String,
    SupplyVendorSiteId: String,
    MatchedFoldPosition: Int,
    AdWidthInPixels: String,
    AdHeightInPixels: String,
    UserHourOfWeek: Int,
    sin_hour_day: Double,
    cos_hour_day: Double,
    sin_hour_week: Double,
    cos_hour_week: Double,
    sin_minute_hour: Double,
    cos_minute_hour: Double,
    PrivateContractId: String,
    Latitude: Double,
    Longitude: Double,
    // ProviderId: String,
    ContextualCategories: Option[Seq[Long]],
    Age: Int,
    Gender: Int,
    AgeGender: Int
    // LabelName: String,
    // LabelValue: Int
)

// COMMAND ----------

// MAGIC %md ModelInputTransform

// COMMAND ----------

object ModelInputTransform extends Logger {

  val flatten_set = Seq("AdsTxtSellerType","PublisherType", "DeviceType", "OperatingSystemFamily", "Browser", "RenderingContext", "DoNotTrack")

  val STRING_FEATURE_TYPE = "string"
  val INT_FEATURE_TYPE = "int"
  val FLOAT_FEATURE_TYPE = "float"
  val ARRAY_INT_FEATURE_TYPE = "array_int"
  val ARRAY_FLOAT_FEATURE_TYPE = "array_float"

  val modelFeatures: Array[ModelFeature] = Array(
    // and this one
    // ModelFeature("AdFormat", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("AdgroupId", STRING_FEATURE_TYPE, Some(38002), 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("AdsTxtSellerType", INT_FEATURE_TYPE, Some(7), 0),
    ModelFeature("PublisherType", INT_FEATURE_TYPE, Some(7), 0),

    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("Metro", STRING_FEATURE_TYPE, Some(302), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(150002), 0),
    ModelFeature("Zip", STRING_FEATURE_TYPE, Some(90002), 0),

    ModelFeature("Browser", INT_FEATURE_TYPE, Some(20), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(6002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(40002), 0),
    ModelFeature("DeviceType", INT_FEATURE_TYPE, Some(9), 0),

    ModelFeature("CreativeId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("DoNotTrack", INT_FEATURE_TYPE, Some(4), 0), // need jiaxing input on that
    ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("OperatingSystemFamily", INT_FEATURE_TYPE, Some(10), 0),
    ModelFeature("RenderingContext", INT_FEATURE_TYPE, Some(6), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(1000), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(500002), 0),
    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(200002), 0),
    ModelFeature("SupplyVendorSiteId", STRING_FEATURE_TYPE, Some(960002), 0),
    ModelFeature("MatchedFoldPosition", INT_FEATURE_TYPE, Some(3), 0),
    ModelFeature("UserHourOfWeek", INT_FEATURE_TYPE, Some(24 * 7 + 2), 0),

    ModelFeature("sin_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_day", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_minute_hour", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("sin_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("cos_hour_week", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("Latitude", FLOAT_FEATURE_TYPE, None, 0),
    ModelFeature("Longitude", FLOAT_FEATURE_TYPE, None, 0),

    // should this have a cardinality?
    ModelFeature("PrivateContractId", STRING_FEATURE_TYPE, Some(10002), 0),
    ModelFeature("ContextualCategories", ARRAY_INT_FEATURE_TYPE, Some(700), 0), 
    // ModelFeature("ProviderId", STRING_FEATURE_TYPE, Some(15), 0)

  )

  // return training input based on bidimps combined dataset
  // if filterresults = true, adgroupfilter must be provided & output will be filtered.
  def transform(demographicBidsImpDF: DataFrame,
                filterResults: Boolean = false  
               ): (DataFrame, DataFrame) = {
    val flattenDF = flattenData(demographicBidsImpDF, flatten_set).selectAs[ModelInputRecord]
    val labelCounts = flattenDF.groupBy("Country", "AgeGender").count()
    val hashedDF = getHashedData(flattenDF)
    (hashedDF, labelCounts)
  }

  def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _) => col(name).alias(name)
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _) => when(col(name).isNotNull, shiftModArrayUdf(col(name), lit(cardinality))).otherwise(col(name)).alias(name)
    }.toArray
  }
  
  def getHashedData(flatten: Dataset[ModelInputRecord]): DataFrame ={
    val selectionQuery = intModelFeaturesCols(modelFeatures) ++ Array(col("Age")) ++ Array(col("Gender")) ++ Array(col("AgeGender")) // ++ Seq("ContextualCategories").map(col)
    flatten.select(selectionQuery: _*)
  }
  
}
