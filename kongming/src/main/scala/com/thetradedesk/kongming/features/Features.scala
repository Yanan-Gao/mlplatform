package com.thetradedesk.kongming.features

import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.{ModelFeature, ModelFeatureLists}
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Features {

  val defaultFeaturesJsonS3Location = "s3://thetradedesk-mlplatform-us-east-1/features/data/kongming/v=1/prod/features/feature.json"

  // TODO: set path for roas' feature json file
  val modelFeaturesTargets: ModelFeatureLists = loadModelFeaturesSplit(config.getString("featuresJson", defaultFeaturesJsonS3Location))

  lazy val modelFeatures: Array[ModelFeature] = modelFeaturesTargets.bidRequest.toArray

  lazy val modelDimensions: Array[ModelFeature] = modelFeaturesTargets.adGroup.toArray

  lazy val seqFields: Array[ModelFeature] = (modelFeaturesTargets.bidRequest ++ modelFeaturesTargets.adGroup)
                                              .foldLeft(Array.empty[ModelFeature]) { (acc, feature) =>
                                                feature match {
                                                  case ModelFeature(_, ARRAY_INT_FEATURE_TYPE, _, _) => acc :+ feature
                                                  case ModelFeature(_, ARRAY_FLOAT_FEATURE_TYPE, _, _) => acc :+ feature
                                                  case _ => acc
                                                }
                                              }

  val directFields = Array(
    ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(500002), 1)
  )

  // Useful fields for analysis/offline attribution. Available everywhere except for the production trainset to minimise
  // data size
  val keptFields = Array(
    ModelFeature("BidRequestId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("CampaignId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("IsTracked", INT_FEATURE_TYPE, None, 0),
    ModelFeature("IsUID2", INT_FEATURE_TYPE, None, 0),
  )

  val modelWeights: Array[ModelFeature] = Array(ModelFeature("Weight", FLOAT_FEATURE_TYPE, None, 0))

  def seqModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _) =>
        (0 until cardinality).map(c => when(col(name).isNotNull && size(col(name)) > c, col(name)(c)).otherwise(0).alias(name + s"_Column$c"))
    }.toArray.flatMap(_.toList)
  }

  def seqModelFeaturesColNames(features: Seq[ModelFeature]): Array[String] = {
    features.map(f => f.name).toArray
  }

  def aliasedModelFeatureCols(modelFeatures: Seq[ModelFeature]): Array[Column] = {
    modelFeatures.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _) =>
        (0 until cardinality).map(c => when(col(name).isNotNull && size(col(name)) > c, col(name)(c)).otherwise(0).alias(name + s"_Column$c"))
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _) => Seq(col(name).alias(name + "Str"))
      case ModelFeature(name, _, _, _) => Seq(col(name))
    }.toArray.flatMap(_.toList)
  }

  def aliasedModelFeatureNames(modelFeatures: Seq[ModelFeature]): Array[String] = {
    modelFeatures.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _) =>
        (0 until cardinality).map(c => name + s"_Column$c")
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _) => Seq(name + "Str")
      case ModelFeature(name, _, _, _) => Seq(name)
    }.toArray.flatMap(_.toList)
  }

  def rawModelFeatureCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map(f => col(f.name)).toArray
  }

  def rawModelFeatureNames(features: Seq[ModelFeature]): Array[String] = {
    features.map(f => f.name).toArray
  }

  case class ModelTarget(name: String, dtype: String, nullable: Boolean)

  val defaultModelTargets = Vector(
    ModelTarget("Target", "Float", nullable = false)
  )

  lazy val modelTargets: Vector[ModelTarget] = modelFeaturesTargets.target match {
    case Some(seq) if seq.isEmpty => defaultModelTargets
    case Some(seq) => seq.toVector.map(feature => ModelTarget(feature.name, feature.dtype, nullable = false))
    case None => defaultModelTargets
  }

  def modelTargetCols(targets: Seq[ModelTarget]): Array[Column] = {
    targets.map(t => col(t.name).alias(t.name)).toArray
  }
}