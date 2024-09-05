package com.thetradedesk.kongming.features

import com.thetradedesk.geronimo.shared._
import com.thetradedesk.geronimo.shared.schemas.{ModelFeature, ModelFeatureLists, Shape}
import com.thetradedesk.kongming.task
import com.thetradedesk.spark.util.TTDConfig.config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Features {

  val defaultFeaturesJsonS3Location = "s3://thetradedesk-mlplatform-us-east-1/features/data/kongming/v=1/prod/features/feature_userdata_group_featuretable.json"
  val defaultROASFeaturesJsonS3Location = "s3://thetradedesk-mlplatform-us-east-1/features/data/roas/v=1/prod/schemas/feature_roas_aud.json"

  var featuresJsonS3Location: String = task match {
    case "roas" => config.getString("featuresJson", defaultROASFeaturesJsonS3Location)
    case _ => config.getString("featuresJson", defaultFeaturesJsonS3Location)
  }
  val modelFeaturesTargets: ModelFeatureLists = parseModelFeaturesSplitFromJson(readModelFeatures(featuresJsonS3Location))

  lazy val userFeatures: Array[ModelFeature] = modelFeaturesTargets.userData.getOrElse(Seq.empty[ModelFeature]).toArray

  lazy val modelFeatures: Array[ModelFeature] = modelFeaturesTargets.bidRequest.toArray ++ userFeatures

  lazy val modelDimensions: Array[ModelFeature] = modelFeaturesTargets.adGroup.toArray

  lazy val seqDirectFields: Array[ModelFeature] = (modelFeaturesTargets.bidRequest ++ userFeatures ++ modelFeaturesTargets.adGroup)
    .foldLeft(Array.empty[ModelFeature]) { (acc, feature) =>
      feature match {
        case ModelFeature(_, ARRAY_INT_FEATURE_TYPE, _, _, _) => acc :+ feature
        case ModelFeature(_, ARRAY_FLOAT_FEATURE_TYPE, _, _, _) => acc :+ feature
        case _ => acc
      }
    }
  lazy val seqHashFields: Array[ModelFeature] = (modelFeaturesTargets.bidRequest ++ modelFeaturesTargets.userData.getOrElse(Seq.empty[ModelFeature]) ++ modelFeaturesTargets.adGroup)
    .foldLeft(Array.empty[ModelFeature]) { (acc, feature) =>
      feature match {
        case ModelFeature(_, ARRAY_LONG_FEATURE_TYPE, _, _, _) => acc :+ feature
        case _ => acc
      }
    }


  // Useful fields for analysis/offline attribution that will be propagated to the full production trainset
  val directFields = Array(
    ModelFeature("AdGroupId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("CampaignId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("AdvertiserId", STRING_FEATURE_TYPE, None, 0),
    //ModelFeature("ImpressionPlacementId", STRING_FEATURE_TYPE, Some(500002), 1)
  )

  val flagFields = Array(
    ModelFeature("UserDataOptIn", INT_FEATURE_TYPE, Some(3), 1, Some(Shape(Seq(1)))),
  )

  // Useful fields for analysis/offline attribution that will not be propagated to the full production trainset to minimise data size
  val keptFields = Array(
    ModelFeature("BidRequestId", STRING_FEATURE_TYPE, None, 0),
    ModelFeature("IsTracked", INT_FEATURE_TYPE, None, 0),
//    ModelFeature("IndustryCategoryId", INT_FEATURE_TYPE, None, 0),
//    ModelFeature("AudienceId", ARRAY_INT_FEATURE_TYPE, Some(30), 0, Some(Shape(Seq(30))))
  )

  val modelWeights: Array[ModelFeature] = Array(ModelFeature("Weight", FLOAT_FEATURE_TYPE, None, 0))

  def seqModelFeaturesCols(inputColAndDims: Seq[ModelFeature]): Array[Column] = {
    inputColAndDims.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _, _) =>
        (0 until cardinality).map(c => when(col(name).isNotNull && size(col(name)) > c, col(name)(c)).otherwise(0).alias(name + s"_Column$c"))
    }.toArray.flatMap(_.toList)
  }

  def seqModelFeaturesColNames(features: Seq[ModelFeature]): Array[String] = {
    features.map(f => f.name).toArray
  }

  def seqModModelFeaturesCols(features: Seq[ModelFeature]): Array[Column] = {
    features.map{
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, Some(shape)) =>
        (0 until shape.dimensions(0)).map(c => when(col(name).isNotNull && size(col(name)) > c, shiftModUdf(col(name)(c), lit(cardinality))).otherwise(0).alias(name + s"_Column$c"))
    }.toArray.flatMap(_.toList)
  }

  def aliasedModelFeatureCols(modelFeatures: Seq[ModelFeature]): Array[Column] = {
    modelFeatures.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _, Some(shape)) =>
        (0 until shape.dimensions(0)).map(c => when(col(name).isNotNull && size(col(name)) > c, col(name)(c)).otherwise(0).alias(name + s"_Column$c"))
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, Some(shape)) =>
        (0 until shape.dimensions(0)).map(c => when(col(name).isNotNull && size(col(name)) > c, col(name)(c)).otherwise(0).alias(name + s"_Column$c"))
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _, _) => Seq(col(name).alias(name + "Str"))
      case ModelFeature(name, _, _, _, _) => Seq(col(name))
    }.toArray.flatMap(_.toList)
  }

  def aliasedModelFeatureNames(modelFeatures: Seq[ModelFeature]): Array[String] = {
    modelFeatures.map {
      case ModelFeature(name, ARRAY_INT_FEATURE_TYPE, Some(cardinality), _, Some(shape)) =>
        (0 until shape.dimensions(0)).map(c => name + s"_Column$c")
      case ModelFeature(name, ARRAY_LONG_FEATURE_TYPE, Some(cardinality), _, Some(shape)) =>
        (0 until shape.dimensions(0)).map(c => name + s"_Column$c")
      case ModelFeature(name, STRING_FEATURE_TYPE, _, _, _) => Seq(name + "Str")
      case ModelFeature(name, _, _, _, _) => Seq(name)
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
