package com.thetradedesk.kongming.transform

import com.thetradedesk.geronimo.shared.{INT_FEATURE_TYPE, STRING_FEATURE_TYPE, intModelFeaturesCols}
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.kongming.datasets.{BidsImpressionsSchema, TrainSetFeatureMappingDataset, TrainSetFeatureMappingRecord}
import com.thetradedesk.spark.TTDSparkContext.spark
import com.thetradedesk.spark.TTDSparkContext.spark.implicits._
import com.thetradedesk.spark.sql.SQLFunctions.DataSetExtensions
import com.thetradedesk.spark.util.prometheus.PrometheusClient
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate

final case class BidsImpressionsFeatureMappingRecord(
                                               // original columns
                                               OriginalAdGroupId: Option[String],
                                               OriginalCampaignId: Option[String],
                                               OriginalAdvertiserId: Option[String],
                                               OriginalSupplyVendor: Option[String],
                                               OriginalSupplyVendorPublisherId: Option[String],
                                               OriginalAliasedSupplyPublisherId: Option[Int],
                                               OriginalSite: Option[String],

                                               OriginalCountry: Option[String],
                                               OriginalRegion: Option[String],
                                               OriginalCity: Option[String],

                                               OriginalDeviceMake: Option[String],
                                               OriginalDeviceModel: Option[String],
                                               OriginalRequestLanguages: String,

                                               // hashed columns
                                               HashedAdGroupId: Int,
                                               HashedCampaignId: Int,
                                               HashedAdvertiserId: Int,
                                               HashedSupplyVendor: Option[Int],
                                               HashedSupplyVendorPublisherId: Option[Int],
                                               HashedAliasedSupplyPublisherId: Option[Int],
                                               HashedSite: Option[Int],

                                               HashedCountry: Option[Int],
                                               HashedRegion: Option[Int],
                                               HashedCity: Option[Int],

                                               HashedDeviceMake: Option[Int],
                                               HashedDeviceModel: Option[Int],
                                               HashedRequestLanguages: Int,
                                          )

object TrainSetFeatureMappingTransform {

  // TODO: hard coded features may conflict with features.json. Need file validation or some other method of verifying
  val mappingModelFeatures: Array[ModelFeature] = Array(
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("CampaignId", STRING_FEATURE_TYPE, Some(5002), 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, Some(500002), 0),
    ModelFeature("SupplyVendor", STRING_FEATURE_TYPE, Some(102), 0),
    ModelFeature("SupplyVendorPublisherId", STRING_FEATURE_TYPE, Some(200002), 0),
    ModelFeature("AliasedSupplyPublisherId", INT_FEATURE_TYPE, Some(200002), 0),
    ModelFeature("Site", STRING_FEATURE_TYPE, Some(500002), 0),

    ModelFeature("Country", STRING_FEATURE_TYPE, Some(252), 0),
    ModelFeature("Region", STRING_FEATURE_TYPE, Some(4002), 0),
    ModelFeature("City", STRING_FEATURE_TYPE, Some(150002), 0),
    ModelFeature("DeviceMake", STRING_FEATURE_TYPE, Some(6002), 0),
    ModelFeature("DeviceModel", STRING_FEATURE_TYPE, Some(40002), 0),
    ModelFeature("RequestLanguages", STRING_FEATURE_TYPE, Some(5002), 0),
  )

  val originalFeatureNamePrefix = "Original"
  val hashedFeatureNamePrefix = "Hashed"

  def dailyTransform(date: LocalDate,
                     bidsImpressions: Dataset[BidsImpressionsSchema]
                    )
                    (implicit prometheus: PrometheusClient): Dataset[TrainSetFeatureMappingRecord] = {
    // duplicate columns for later hashing
    var bidsImpressionsDF = bidsImpressions.toDF()
    mappingModelFeatures.foreach { f =>
      bidsImpressionsDF = bidsImpressionsDF.withColumn(renameOriginalModelFeatureCol(f.name), col(f.name))
    }
    // keep columns whose name starts with "Original" when doing future mapping
    val mappingKeptFields = mappingModelFeatures.map(f => ModelFeature(renameOriginalModelFeatureCol(f.name), f.dtype, f.cardinality, f.modelVersion))
    // hash columns of impressions
    val hashTabular = intModelFeaturesCols(mappingModelFeatures) ++ keepFeatureCols(mappingKeptFields)
    // rename feature columns with a prefix of "hashed"
    val renameTabular = renameHashedModelFeatureCols(mappingModelFeatures) ++ keepFeatureCols(mappingKeptFields)
    // map columns
    val bidsImpressionsMappings = bidsImpressionsDF.select(hashTabular: _*).select(renameTabular: _*).selectAs[BidsImpressionsFeatureMappingRecord]

    // get mappings for each feature
    var dailyFeatureMappings = spark.emptyDataset[TrainSetFeatureMappingRecord]
    mappingModelFeatures.foreach { f =>
      dailyFeatureMappings = dailyFeatureMappings.union(featureMappingTransform(bidsImpressionsMappings, f.name))
    }
    // union existing mappings with latest mappings and return distinct mappings
    val existingFeatureMappings = TrainSetFeatureMappingDataset().readLatestPartitionUpTo(date, true).selectAs[TrainSetFeatureMappingRecord]
    existingFeatureMappings.union(dailyFeatureMappings).distinct()
  }

  def keepFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name)).toArray
  }

  def renameHashedModelFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name).alias(renameHashedModelFeatureCol(f.name))).toArray
  }

  def renameHashedModelFeatureCol(featureName : String) : String = {
    hashedFeatureNamePrefix + featureName
  }
  def renameOriginalModelFeatureCol(featureName: String): String = {
    originalFeatureNamePrefix + featureName
  }

  def featureMappingTransform(featureMappings: Dataset[BidsImpressionsFeatureMappingRecord],
                           featureName: String
                          ): Dataset[TrainSetFeatureMappingRecord] = {
    val originalFeatureName = renameOriginalModelFeatureCol(featureName)
    val hashedFeatureName = renameHashedModelFeatureCol(featureName)
    // select distinct feature mappings
    featureMappings.select(originalFeatureName, hashedFeatureName).distinct()
      .withColumn("FeatureName", lit(featureName))
      .withColumnRenamed(originalFeatureName, "FeatureOriginalValue" )
      .withColumnRenamed(hashedFeatureName, "FeatureHashedValue").selectAs[TrainSetFeatureMappingRecord]
  }

  def tryGetFeatureCardinality(name: String): Int = {
    for (feature <- mappingModelFeatures) {
      if (feature.name == name) {
        if (feature.cardinality.isEmpty) {
          throw new RuntimeException("Feature is not categorical: " + name)
        }

        return feature.cardinality.get
      }
    }

    throw new RuntimeException("Feature not found in TrainSetFeatureMappingTransform.mappingModelFeatures: " + name)
  }

}
