package com.thetradedesk.philo.transform

import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.ModelFeature
import com.thetradedesk.spark.sql.SQLFunctions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, size, when, xxhash64}

/**
 * Feature transformation utilities for model input processing.
 * Handles hashing, encoding, and transformation of different feature types.
 */
object FeatureTransform {

  /**
   * Transform integer, string, and float model features with appropriate hashing.
   * 
   * @param inputColAndDims sequence of model features to process
   * @return array of transformed columns
   */
  def intModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, STRING_FEATURE_TYPE, Some(cardinality), _, _, _) => 
        when(col(name).isNotNullOrEmpty, shiftModUdf(xxhash64(col(name)), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, INT_FEATURE_TYPE, Some(cardinality), _, _, _) => 
        when(col(name).isNotNull, shiftModUdf(col(name), lit(cardinality))).otherwise(0).alias(name)
      case ModelFeature(name, FLOAT_FEATURE_TYPE, _, _, _, _) => 
        col(name).alias(name)
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _, _, _) => 
        when(col(name).isNotNull, shiftModArrayUdf(col(name), lit(cardinality))).otherwise(col(name)).alias(name)
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, _, _) => 
        when(col(name).isNotNull, shiftModArrayUdf(col(name), lit(cardinality))).otherwise(lit(null)).alias(name)
      case ModelFeature(name, ARRAY_FLOAT_FEATURE_TYPE, _, _, _, _) => 
        col(name).alias(name)
    }.toArray
  }

  /**
   * Transform sequence/array model features with shape-based column expansion.
   * 
   * @param features sequence of array-type model features
   * @return array of transformed columns with expanded dimensions
   */
  def seqModModelFeaturesCols(features: Seq[ModelFeature]): Array[Column] = {
    features.flatMap {
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, Some(shape), _) =>
        (0 until shape.dimensions(0)).map(c => 
          when(col(name).isNotNull && size(col(name)) > c, 
               shiftModUdf(col(name)(c), lit(cardinality))
          ).otherwise(0).alias(name + s"_Column$c")
        )
      case _ => 
        // Skip features that don't match the expected pattern
        Seq.empty[Column]
    }.toArray
  }

  /**
   * Apply feature hashing and selection to create the final model input dataset.
   * 
   * @param flatten input DataFrame with flattened features
   * @param hashFeatures sequence of features to hash
   * @param seqHashFields sequence of array features to process
   * @param originalColNames original column names to preserve
   * @param addCols additional columns to include
   * @return DataFrame with hashed features and selected columns
   */
  def getHashedData(flatten: DataFrame, 
                    hashFeatures: Seq[ModelFeature], 
                    seqHashFields: Seq[ModelFeature],
                    originalColNames: Seq[String], 
                    addCols: Seq[String]): DataFrame = {
    val origCols = addCols ++ originalColNames
    // TODO: we need a better way to track these fields
    val selectionQuery = intModelFeaturesCols(hashFeatures) ++ 
                        seqModModelFeaturesCols(seqHashFields) ++ 
                        origCols.map(col)

    flatten.select(selectionQuery: _*)
  }

  /**
   * Mask sensitive features based on restriction flags.
   * Currently not used but available for privacy protection.
   * 
   * @param df input DataFrame
   * @param featureName name of feature to mask
   * @return DataFrame with masked features
   */
  def maskFeatures(df: DataFrame, featureName: String): DataFrame = {
    df.withColumn(featureName, when(col("IsRestricted") === 1, 0).otherwise(col(featureName)))
  }
}