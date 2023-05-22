package com.thetradedesk.pythia.interestModel.transform

import com.thetradedesk.geronimo.bidsimpression.schema.{BidsImpressions, BidsImpressionsSchema}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.geronimo.shared.{GERONIMO_DATA_SOURCE, explicitDatePart, paddedDatePart, parquetDataPaths, loadParquetData, shiftMod, shiftModUdf, intModelFeaturesCols}
import com.thetradedesk.spark.util.io.FSUtils

import com.thetradedesk.geronimo.shared.schemas.{AdsTxtSellerTypeLookupRecord, BrowserLookupRecord, DeviceTypeLookupRecord, DoNotTrackLookupRecord, InternetConnectionRecord, InventoryPublisherTypeLookupRecord, OSFamilyLookupRecord, PredictiveClearingModeLookupRecord, RenderingContextLookupRecord, OSLookupRecord}
import com.thetradedesk.geronimo.shared.{FLOAT_FEATURE_TYPE, INT_FEATURE_TYPE, STRING_FEATURE_TYPE }
import com.thetradedesk.pythia.interestModel.schema.{ModelInputRecord, ModelInputDataset}

import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.util.TTDConfig.config
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.sql.SQLFunctions._
import com.thetradedesk.logging.Logger

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.time.LocalDate

// ---------------------------------------
// TRANSFORMATION OF GERONIMO DATA INTO INTEREST MODEL INPUT FORMAT
// ---------------------------------------
object ModelInputTransform extends Logger {
  // ------------------------------
  // Main method to transform the BidsImpressions data into interest model input format
  def transform(
    bidsImpsDat: Dataset[BidsImpressionsSchema],
    dfTaxMapFull: DataFrame,
    modelFeatures: Seq[ModelFeature]
  ): (DataFrame, Seq[String]) = {

    // Filtering bidsImpressions and flattening certain columns  
    val dfFormattedBidsImps = formatBidsImps(bidsImpsDat)

    // Adding interest labels to the bidsimpressions data
    val dfDataWithLabels = addLabelsToData(dfFormattedBidsImps, dfTaxMapFull)

    // Filtering down to only data with at least one positive interest label among the whole set
    val dfDataWithPositiveLabelPresence = filterByPositiveLabelPresence(dfDataWithLabels)

    // Selecting model input records
    val dfOutputSelect = dfDataWithPositiveLabelPresence.selectAs[ModelInputRecord]
    
    // Hashing all categorical features and selecting label + feature columns
    val dfHashedDataOutput = getHashedData(dfOutputSelect.selectAs[ModelInputRecord], labelColumn = "Labels", modelFeatures)

    // Sampling the data
    val dfHashedWithLabelSampled = dfHashedDataOutput.sample(fraction = 0.1, seed = 28347)

    val columnsLabels = dfDataWithLabels.columns.filter(name => name.startsWith("label_"))

    (dfHashedWithLabelSampled, columnsLabels)
  }

  // ------------------------------
  // Filtering bidsImpressions and flattening certain columns
  def formatBidsImps(dfBidsImps: Dataset[BidsImpressionsSchema]): DataFrame = {
    dfBidsImps
      // Only keep trackable bid requests
      .filter($"DoNotTrack".apply("value") === 0)

      // Only keep entries where we'd be able to find a label
      .filter($"ThirdPartyTargetingDataIds".isNotNull)

      // Un-nesting the columns that contain dictionary values (int instead of strings)
      .withColumnRenamed("RenderingContext", "RenderingContextNested")
      .withColumn("RenderingContext", $"RenderingContextNested".apply("value"))
      .withColumnRenamed("DeviceType", "DeviceTypeNested")
      .withColumn("DeviceType", $"DeviceTypeNested".apply("value"))
      .withColumnRenamed("OperatingSystem", "OperatingSystemNested")
      .withColumn("OperatingSystem", $"OperatingSystemNested".apply("value"))
      .withColumnRenamed("OperatingSystemFamily", "OperatingSystemFamilyNested")
      .withColumn("OperatingSystemFamily", $"OperatingSystemFamilyNested".apply("value"))
      .withColumnRenamed("Browser", "BrowserNested")
      .withColumn("Browser", $"BrowserNested".apply("value"))
      .withColumnRenamed("InternetConnectionType", "InternetConnectionTypeNested")
      .withColumn("InternetConnectionType", $"InternetConnectionTypeNested".apply("value"))
      .withColumnRenamed("PublisherType", "PublisherTypeNested")
      .withColumn("PublisherType", $"PublisherTypeNested".apply("value"))
      .drop("RenderingContextNested", "DeviceTypeNested",
            "OperatingSystemNested",  "OperatingSystemFamilyNested",
            "BrowserNested",          "InternetConnectionTypeNested",
            "PublisherTypeNested")
  }
  
  // ------------------------------
  // Generating 0/1 label columns based on the mapped targetingDataIds
  def addLabelsToData(
    dfBidsImps: DataFrame,
    //arrayTaxMapLabels: Array[(String, Any)]
    dfTaxMap: DataFrame
  ): DataFrame = {
    var dfDataWithLabels = dfBidsImps
    
    // Stripping down the taxonomy map to the relevant bit for label generation and outputting an array of targetingDataIds, corresponding to the labels
    val dfTaxMapLabels = dfTaxMap
      .filter($"selected" === 1)
      .select("IABAudienceV11_UniqueId", // for the label column name
              "targetingDataId_25"       // using the "Top 25%" percentile interest segments for label generation
             )
      .filter($"targetingDataId_25".isNotNull)
      .withColumnRenamed("targetingDataId_25", "labelTargetingDataId")
      .orderBy("IABAudienceV11_UniqueId")
    
    // Converting the IAB to TTD taxonomy mapping data frame into an array of tuples
    val arrayTaxMapLabels = dfTaxMapLabels.rdd.map(x => (x.get(0).toString, x.get(1))).collect()

    // Generating 0/1 label columns based on the "Top 25%" percentile interest segments
    arrayTaxMapLabels.foreach{ case (col1, col2) =>
      dfDataWithLabels = dfDataWithLabels
        .withColumn("label_" + col1, 
                    when(array_contains($"ThirdPartyTargetingDataIds", col2),1).otherwise(0) ) 
    }

    // Aggregating those label columns into one array column "Labels"
    val columnsLabelsAll = dfDataWithLabels.columns.filter(name => name.startsWith("label_"))
    
    val columnsLabels = dfTaxMapLabels.select("IABAudienceV11_UniqueId").rdd.map(r => r(0)).collect.toList.map(_.toString).map(i=> "label_"+i).toSeq // selecting the columns in the order that they appear in the ordered dfTaxMapLabels

    dfDataWithLabels = dfDataWithLabels.withColumn("Labels", array(columnsLabels.map(dfDataWithLabels(_)):_*))

    dfDataWithLabels
  }

  // ------------------------------
  // Filtering down to only those entries that have at least one positive label entry among them all
  def filterByPositiveLabelPresence(dfDataWithLabels: DataFrame): DataFrame = {
    // Creating an indicator whether an entry has at least one positive label
    val dfDataWithLabelsAndIndicator = dfDataWithLabels
      .withColumn("hasLabel",
                  when(array_contains($"Labels", 1), 1).otherwise(0)
                 )
    // Filtering down to only data with labels
    val dfFilteredByHasLabel = dfDataWithLabelsAndIndicator
      .filter($"hasLabel" === 1)

    dfFilteredByHasLabel
  }
  
  // ------------------------------
  // Function to hash all columns that are to be hashed & select selection of columns
  def getHashedData(df: Dataset[ModelInputRecord], labelColumn: String, modelFeatures: Seq[ModelFeature]): DataFrame ={
    val selectionQuery = Array(col(labelColumn)) ++ intModelFeaturesCols(modelFeatures)
    df.select(selectionQuery: _*)
  }
}