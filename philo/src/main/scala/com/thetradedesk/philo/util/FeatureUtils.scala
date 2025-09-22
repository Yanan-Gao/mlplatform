package com.thetradedesk.philo.util

import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.ModelFeature

/**
 * Feature processing utilities for model feature management.
 */
object FeatureUtils {

  /**
   * Optional feature mapping configuration.
   */
  val optionalFeature: Map[Int, String] = Map(
    0 -> "UserData"
  )

  /**
   * Generate aliased model feature names for array features.
   * For array features, creates column names with indices (e.g., "feature_Column0", "feature_Column1").
   * 
   * @param modelFeatures sequence of model features to process
   * @return array of aliased feature names
   */
  def aliasedModelFeatureNames(modelFeatures: Seq[ModelFeature]): Array[String] = {
    modelFeatures.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _, Some(shape), _) =>
        (0 until shape.dimensions(0)).map(c => name + s"_Column$c")
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, Some(shape), _) =>
        (0 until shape.dimensions(0)).map(c => name + s"_Column$c")
      case ModelFeature(name, _, _, _, _, _) => Seq(name)
    }.toArray.flatMap(_.toList)
  }

  /**
   * Generate original column names with "original" prefix.
   * 
   * @param keptCols sequence of column names
   * @return array of column names with "original" prefix
   */
  def addOriginalNames(keptCols: Seq[String]): Array[String] = {
    keptCols.map { c => s"original$c" }.toArray
  }
}