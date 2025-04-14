package com.thetradedesk.featurestore.jobs

import com.thetradedesk.featurestore.features.Features.{AggFunc, CategoryFeatAggSpecs, ContinuousFeatAggSpecs, RatioFeatAggSpecs}
import scala.io.Source
import io.circe._
import io.circe.generic.auto._

abstract class FeatureStoreJobConfig(val configFile: String) {
  // Method to read the JSON file
  private def readJsonFile(configFile: String): String = {
    val basePath = "jobconfigs"
    val resourcePath = s"$basePath/$configFile"
    try {
      // Get the resource as an InputStream
      val resourceStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
      if (resourceStream == null) {
        println(s"Resource not found: $resourcePath")
      }
      Source.fromInputStream(resourceStream).mkString
    } catch {
      case e: Exception => throw e
    }
  }

  val jsonString: String = readJsonFile(configFile)

  val parsed: Either[ParsingFailure, Json] = parser.parse(jsonString)
}

class FeatureStoreAggJobConfig(override val configFile: String) extends FeatureStoreJobConfig(configFile) {

  val config: Either[Error, AggJobConfig] = parsed.flatMap(_.as[AggJobConfig])

  def catFeatSpecs: Array[CategoryFeatAggSpecs] = config match {
    case Right(config) =>
      config.categorical.getOrElse(Array.empty).flatMap { f =>
        f.aggWindows.map { window =>
          CategoryFeatAggSpecs(aggField = f.aggField, aggWindow = window, topN = f.topN, dataType = f.dataType, cardinality = f.cardinality)
        }
      }
    case Left(error) =>
      println(s"Failed to parse FeatureStoreAggJobConfig JSON file: $error")
      Array.empty
  }

  def conFeatSpecs: Array[ContinuousFeatAggSpecs] = config match {
    case Right(config) =>
      config.continuous.getOrElse(Array.empty).flatMap { f =>
        f.aggWindows.map { window =>
          ContinuousFeatAggSpecs(aggField = f.aggField, aggWindow = window, aggFunc = f.aggFunc)
        }
      }
    case Left(error) =>
      println(s"Failed to parse FeatureStoreAggJobConfig JSON file: $error")
      Array.empty
  }

  def ratioFeatSpecs: Array[RatioFeatAggSpecs] = config match {
    case Right(config) =>
      config.ratio.getOrElse(Array.empty).flatMap { f =>
        f.aggWindows.map { window =>
          RatioFeatAggSpecs(aggField = f.aggField, aggWindow = window, denomField = f.denomField, ratioMetrics = f.ratioMetrics)
        }
      }
    case Left(error) =>
      println(s"Failed to parse FeatureStoreAggJobConfig JSON file: $error")
      Array.empty
  }
}

case class AggJobConfig(
  categorical: Option[Array[ParsedCategoricalFeatureSpec]],
  continuous: Option[Array[ParsedContinuousFeatureSpec]],
  ratio: Option[Array[ParsedRatioFeatureSpec]]
)

case class ParsedCategoricalFeatureSpec(
  aggField: String,
  dataType: String,
  aggWindows: Array[Int],
  topN: Int,
  cardinality: Int
)

case class ParsedContinuousFeatureSpec(
  aggField: String,
  aggWindows: Array[Int],
  aggFunc: AggFunc.AggFunc
)

case class ParsedRatioFeatureSpec(
  aggField: String,
  aggWindows: Array[Int],
  denomField: String,
  ratioMetrics: String
)
